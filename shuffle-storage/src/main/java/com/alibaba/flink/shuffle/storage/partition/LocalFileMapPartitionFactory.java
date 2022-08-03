/*
 * Copyright 2021 The Flink Remote Shuffle Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DiskType;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.utils.StorageConfigParseUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.DataInput;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

/** {@link DataPartitionFactory} of {@link LocalFileMapPartition}. */
@NotThreadSafe
public class LocalFileMapPartitionFactory implements DataPartitionFactory {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileMapPartitionFactory.class);

    protected final Object lock = new Object();

    @GuardedBy("lock")
    protected final Queue<StorageMeta> ssdStorageMetas = new ArrayDeque<>();

    @GuardedBy("lock")
    protected final Queue<StorageMeta> hddStorageMetas = new ArrayDeque<>();

    @GuardedBy("lock")
    protected final HashMap<String, ArrayList<StorageMeta>> storageMetasByName = new HashMap<>();

    protected final StorageSpaceInfo storageSpaceInfo = new StorageSpaceInfo(0, 0, 0, 0);

    protected long minReservedSpaceBytes;

    protected long maxUsableSpaceBytes;

    protected StorageType preferredStorageType;

    private boolean isStorageSpaceLimited;

    @Override
    public void initialize(Configuration configuration) {
        isStorageSpaceLimited =
                configuration
                                .getMemorySize(StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES)
                                .getBytes()
                        < MemorySize.MAX_VALUE.getBytes();
        String directories = configuration.getString(StorageOptions.STORAGE_LOCAL_DATA_DIRS);
        if (directories == null) {
            throw new ConfigurationException(
                    StorageOptions.STORAGE_LOCAL_DATA_DIRS.key() + " is not configured.");
        }

        String diskTypeString = configuration.getString(StorageOptions.STORAGE_PREFERRED_TYPE);
        try {
            preferredStorageType =
                    StorageType.valueOf(CommonUtils.checkNotNull(diskTypeString).trim());
        } catch (Exception exception) {
            throw new ConfigurationException(
                    String.format(
                            "Illegal configured value %s for %s. Must be SSD, HDD or UNKNOWN.",
                            diskTypeString, StorageOptions.STORAGE_PREFERRED_TYPE.key()));
        }

        StorageConfigParseUtils.ParsedPathLists parsedPathLists =
                StorageConfigParseUtils.parseStoragePaths(directories);
        if (parsedPathLists.getAllPaths().isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "No valid data dir is configured for %s.",
                            StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
        }

        synchronized (lock) {
            addStorageMetas(ssdStorageMetas, parsedPathLists.getSsdPaths(), StorageType.SSD);
            addStorageMetas(hddStorageMetas, parsedPathLists.getHddPaths(), StorageType.HDD);
        }

        this.minReservedSpaceBytes =
                configuration
                        .getMemorySize(StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES)
                        .getBytes();
        this.maxUsableSpaceBytes =
                configuration
                        .getMemorySize(StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES)
                        .getBytes();

        updateStorageHealthStatus();
        updateFreeStorageSpace();
    }

    private void addStorageMetas(
            Queue<StorageMeta> metaQueue, List<String> storagePaths, StorageType storageType) {
        assert Thread.holdsLock(lock);

        metaQueue.addAll(
                storagePaths.stream()
                        .map(
                                storagePath ->
                                        new LocalFileStorageMeta(
                                                storagePath,
                                                storageType,
                                                getStorageNameFromPath(storagePath)))
                        .collect(Collectors.toList()));

        if (metaQueue.isEmpty() && preferredStorageType == storageType) {
            LOG.warn(
                    "No valid storage of {} type is configured for {}.",
                    storageType,
                    StorageOptions.STORAGE_LOCAL_DATA_DIRS.key());
        }

        for (StorageMeta storageMeta : metaQueue) {
            ArrayList<StorageMeta> metaList =
                    storageMetasByName.computeIfAbsent(
                            storageMeta.getStorageName(), ignored -> new ArrayList<>());
            metaList.add(storageMeta);
        }
    }

    /**
     * Returns the next data path to use for data storage. It serves data path in a simple round
     * robin way. More complicated strategies can be implemented in the future.
     */
    protected StorageMeta getNextDataStorageMeta() {
        switch (preferredStorageType) {
            case SSD:
                {
                    StorageMeta ssdStorageMeta = getNextSsdStorageMeta();
                    if (ssdStorageMeta != null
                            && ssdStorageMeta.getFreeStorageSpace() > minReservedSpaceBytes
                            && ssdStorageMeta.getUsedStorageSpace() < maxUsableSpaceBytes) {
                        return ssdStorageMeta;
                    }
                    StorageMeta hddStorageMeta = getNextHddStorageMeta();
                    if (hddStorageMeta != null
                            && hddStorageMeta.getFreeStorageSpace() > minReservedSpaceBytes
                            && hddStorageMeta.getUsedStorageSpace() < maxUsableSpaceBytes) {
                        return hddStorageMeta;
                    }
                    return ssdStorageMeta != null ? ssdStorageMeta : hddStorageMeta;
                }
            case HDD:
                {
                    StorageMeta hddStorageMeta = getNextHddStorageMeta();
                    if (hddStorageMeta != null
                            && hddStorageMeta.getFreeStorageSpace() > minReservedSpaceBytes
                            && hddStorageMeta.getUsedStorageSpace() < maxUsableSpaceBytes) {
                        return hddStorageMeta;
                    }
                    StorageMeta ssdStorageMeta = getNextSsdStorageMeta();
                    if (ssdStorageMeta != null
                            && ssdStorageMeta.getFreeStorageSpace() > minReservedSpaceBytes
                            && ssdStorageMeta.getUsedStorageSpace() < maxUsableSpaceBytes) {
                        return ssdStorageMeta;
                    }
                    return hddStorageMeta != null ? hddStorageMeta : ssdStorageMeta;
                }
            default:
                throw new ShuffleException("Illegal preferred storage type.");
        }
    }

    private StorageMeta getNextSsdStorageMeta() {
        synchronized (lock) {
            if (ssdStorageMetas.isEmpty()) {
                return null;
            }
            return getStorageMetaInNonEmptyQueue(ssdStorageMetas);
        }
    }

    private StorageMeta getNextHddStorageMeta() {
        synchronized (lock) {
            if (hddStorageMetas.isEmpty()) {
                return null;
            }
            return getStorageMetaInNonEmptyQueue(hddStorageMetas);
        }
    }

    /**
     * Returns 1) null if there is no healthy storage; 2) first storage which meets the reserved
     * space requirement; 3) storage with maximum free space if no storage meets the reserved space
     * requirement.
     */
    protected StorageMeta getStorageMetaInNonEmptyQueue(Queue<StorageMeta> storageMetas) {
        assert Thread.holdsLock(lock);

        int numStorageMetas = storageMetas.size();
        StorageMeta maxFreeSpaceMeta = null;
        for (int i = 0; i < numStorageMetas; i++) {
            StorageMeta storageMeta = storageMetas.poll();
            if (storageMeta == null) {
                continue;
            }

            storageMetas.add(storageMeta);
            if (!storageMeta.isHealthy()) {
                continue;
            }

            long freeSpace = storageMeta.getFreeStorageSpace();
            if (maxFreeSpaceMeta == null || freeSpace > maxFreeSpaceMeta.getFreeStorageSpace()) {
                maxFreeSpaceMeta = storageMeta;
            }

            if (freeSpace > minReservedSpaceBytes
                    && storageMeta.getUsedStorageSpace() < maxUsableSpaceBytes) {
                return storageMeta;
            }
        }

        if (maxFreeSpaceMeta == null || !maxFreeSpaceMeta.isHealthy()) {
            return null;
        }

        if (storageMetas.remove(maxFreeSpaceMeta)) {
            storageMetas.add(maxFreeSpaceMeta);
        }
        return maxFreeSpaceMeta;
    }

    @Override
    public LocalFileMapPartition createDataPartition(
            PartitionedDataStore dataStore,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            int numMapPartitions,
            int numReducePartitions) {
        CommonUtils.checkArgument(dataPartitionID != null, "Must be not null.");
        CommonUtils.checkArgument(dataPartitionID instanceof MapPartitionID, "Illegal type.");

        MapPartitionID mapPartitionID = (MapPartitionID) dataPartitionID;
        StorageMeta storageMeta = getNextDataStorageMeta();
        if (storageMeta == null) {
            throw new RuntimeException("No available healthy storage.");
        }
        return new LocalFileMapPartition(
                storageMeta, dataStore, jobID, dataSetID, mapPartitionID, numReducePartitions);
    }

    @Override
    public LocalFileMapPartition createDataPartition(
            PartitionedDataStore dataStore, DataPartitionMeta partitionMeta) {
        CommonUtils.checkArgument(
                partitionMeta instanceof LocalFileMapPartitionMeta, "Illegal data partition type.");

        return new LocalFileMapPartition(dataStore, (LocalFileMapPartitionMeta) partitionMeta);
    }

    @Override
    public LocalFileMapPartitionMeta recoverDataPartitionMeta(DataInput dataInput)
            throws IOException {
        return LocalFileMapPartitionMeta.readFrom(dataInput, this);
    }

    /** At the present, only MAP_PARTITION is supported. */
    @Override
    public DataPartition.DataPartitionType getDataPartitionType() {
        return DataPartition.DataPartitionType.MAP_PARTITION;
    }

    @Override
    public void updateFreeStorageSpace() {
        long maxSsdFreeSpaceBytes = 0;
        for (StorageMeta storageMeta : getSsdStorageMetas()) {
            if (!storageMeta.isHealthy()) {
                continue;
            }
            long freeSpaceBytes = storageMeta.updateFreeStorageSpace();
            if (freeSpaceBytes > maxSsdFreeSpaceBytes) {
                maxSsdFreeSpaceBytes = freeSpaceBytes;
            }
        }
        storageSpaceInfo.setSsdMaxFreeSpaceBytes(maxSsdFreeSpaceBytes);

        long maxHddFreeSpaceBytes = 0;
        for (StorageMeta storageMeta : getHddStorageMetas()) {
            if (!storageMeta.isHealthy()) {
                continue;
            }
            long freeSpaceBytes = storageMeta.updateFreeStorageSpace();
            if (freeSpaceBytes > maxHddFreeSpaceBytes) {
                maxHddFreeSpaceBytes = freeSpaceBytes;
            }
        }
        storageSpaceInfo.setHddMaxFreeSpaceBytes(maxHddFreeSpaceBytes);
    }

    @Override
    public StorageSpaceInfo getStorageSpaceInfo() {
        return storageSpaceInfo;
    }

    @Override
    public boolean isStorageSpaceValid(
            StorageSpaceInfo storageSpaceInfo,
            long minReservedSpaceBytes,
            long maxUsableSpaceBytes) {
        return (minReservedSpaceBytes < storageSpaceInfo.getHddMaxFreeSpaceBytes()
                        && maxUsableSpaceBytes > storageSpaceInfo.getHddMaxUsedSpaceBytes())
                || (minReservedSpaceBytes < storageSpaceInfo.getSsdMaxFreeSpaceBytes()
                        && maxUsableSpaceBytes > storageSpaceInfo.getSsdMaxUsedSpaceBytes());
    }

    @Override
    public void updateStorageHealthStatus() {
        List<StorageMeta> storageMetas = getAllStorageMetas();
        for (StorageMeta storageMeta : storageMetas) {
            storageMeta.updateStorageHealthStatus();
        }
    }

    @Override
    public DiskType getDiskType() {
        return DiskType.ANY_TYPE;
    }

    @Override
    public boolean useSsdOnly() {
        return false;
    }

    @Override
    public boolean useHddOnly() {
        return false;
    }

    @Override
    public String getStorageNameFromPath(String storagePath) {
        try {
            return Files.getFileStore(Paths.get(storagePath)).name();
        } catch (IOException e) {
            if (isStorageSpaceLimited) {
                throw new RuntimeException(
                        String.format(
                                "Failed to get the storage name of path %s, can not limit the max "
                                        + "storage space can be used configured by %s. To avoid "
                                        + "this exception, you can either fix the root cause or "
                                        + "just remove this configuration %s.",
                                storagePath,
                                StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES.key(),
                                StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES.key()),
                        e);
            }
            return storagePath;
        }
    }

    @Override
    public void updateUsedStorageSpace(Map<String, Long> storageUsedBytes) {
        if (storageUsedBytes == null || storageUsedBytes.isEmpty()) {
            return;
        }

        synchronized (lock) {
            for (Map.Entry<String, Long> entry : storageUsedBytes.entrySet()) {
                ArrayList<StorageMeta> metaList = storageMetasByName.get(entry.getKey());
                if (metaList == null || metaList.isEmpty()) {
                    continue;
                }
                metaList.forEach(
                        storageMeta -> storageMeta.updateUsedStorageSpace(entry.getValue()));
            }
        }

        storageSpaceInfo.setSsdMaxUsedSpaceBytes(calculateUsedSpaceBytes(getSsdStorageMetas()));
        storageSpaceInfo.setHddMaxUsedSpaceBytes(calculateUsedSpaceBytes(getHddStorageMetas()));
    }

    private long calculateUsedSpaceBytes(List<StorageMeta> storageMetas) {
        if (storageMetas == null || storageMetas.isEmpty()) {
            return 0;
        }

        long maxUsedSpaceBytes = 0;
        for (StorageMeta storageMeta : storageMetas) {
            maxUsedSpaceBytes = Math.max(maxUsedSpaceBytes, storageMeta.getUsedStorageSpace());
        }
        return maxUsedSpaceBytes;
    }

    protected List<StorageMeta> getSsdStorageMetas() {
        synchronized (lock) {
            return new ArrayList<>(ssdStorageMetas);
        }
    }

    protected List<StorageMeta> getHddStorageMetas() {
        synchronized (lock) {
            return new ArrayList<>(hddStorageMetas);
        }
    }

    private List<StorageMeta> getAllStorageMetas() {
        List<StorageMeta> storageMetas = new ArrayList<>();
        synchronized (lock) {
            storageMetas.addAll(ssdStorageMetas);
            storageMetas.addAll(hddStorageMetas);
        }
        return storageMetas;
    }

    // ---------------------------------------------------------------------------------------------
    // For test
    // ---------------------------------------------------------------------------------------------

    boolean isStorageSpaceLimited() {
        return isStorageSpaceLimited;
    }

    StorageType getPreferredStorageType() {
        return preferredStorageType;
    }

    void addSsdStorageMeta(StorageMeta storageMeta) {
        ssdStorageMetas.add(storageMeta);
    }

    void addHddStorageMeta(StorageMeta storageMeta) {
        hddStorageMetas.add(storageMeta);
    }
}
