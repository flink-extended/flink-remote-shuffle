/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.coordinator.worker.metastore;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.storage.utils.DataPartitionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** The meta store based on storing the meta files on the disk containing the data partitions. */
public class LocalShuffleMetaStore implements Metastore {

    private static final Logger LOG = LoggerFactory.getLogger(LocalShuffleMetaStore.class);

    public static final String META_DIR_NAME = "_meta";

    private final Set<String> storagePaths;

    private final Map<DataPartitionCoordinate, DataPartitionMetaStatus> dataPartitions =
            new HashMap<>();

    @Nullable private volatile BiConsumer<JobID, DataPartitionCoordinate> partitionRemovedConsumer;

    public LocalShuffleMetaStore(Set<String> storagePaths) throws Exception {
        this.storagePaths = checkNotNull(storagePaths);
        initialize();
    }

    private void initialize() {
        for (String base : storagePaths) {
            File metaBaseDir = new File(base, META_DIR_NAME);

            if (!metaBaseDir.exists()) {
                continue;
            }

            List<File> spoiledMetaFiles = new ArrayList<>();
            for (File subFile :
                    Optional.ofNullable(metaBaseDir.listFiles()).orElseGet(() -> new File[0])) {
                try (FileInputStream fileInputStream = new FileInputStream(subFile);
                        DataInputStream dataInput = new DataInputStream(fileInputStream)) {
                    DataPartitionMeta dataPartitionMeta =
                            DataPartitionUtils.deserializePartitionMeta(dataInput);
                    dataPartitions.put(
                            new DataPartitionCoordinate(
                                    dataPartitionMeta.getDataSetID(),
                                    dataPartitionMeta.getDataPartitionID()),
                            new DataPartitionMetaStatus(dataPartitionMeta, false));
                } catch (Exception e) {
                    LOG.warn("Failed to parse " + subFile.getAbsolutePath(), e);
                    spoiledMetaFiles.add(subFile);
                }
            }

            for (File spoiledMetaFile : spoiledMetaFiles) {
                try {
                    boolean deleted = spoiledMetaFile.delete();
                    if (!deleted) {
                        LOG.warn(
                                "Failed to remove the spoiled meta file "
                                        + spoiledMetaFile.getPath());
                    }
                } catch (Exception e) {
                    LOG.warn(
                            "Failed to remove the spoiled meta file " + spoiledMetaFile.getPath(),
                            e);
                }
            }
        }
    }

    @Override
    public void setPartitionRemovedConsumer(
            BiConsumer<JobID, DataPartitionCoordinate> partitionRemovedConsumer) {
        this.partitionRemovedConsumer = checkNotNull(partitionRemovedConsumer);
    }

    @Override
    public List<DataPartitionStatus> listDataPartitions() {
        List<DataPartitionStatus> dataPartitionStatuses = new ArrayList<>();

        synchronized (dataPartitions) {
            dataPartitions.forEach(
                    ((coordinate, dataPartitionWorkerStatus) -> {
                        dataPartitionStatuses.add(
                                new DataPartitionStatus(
                                        dataPartitionWorkerStatus.getMeta().getJobID(),
                                        new DataPartitionCoordinate(
                                                dataPartitionWorkerStatus.getMeta().getDataSetID(),
                                                dataPartitionWorkerStatus
                                                        .getMeta()
                                                        .getDataPartitionID()),
                                        dataPartitionWorkerStatus.isReleasing()));
                    }));
        }

        return dataPartitionStatuses;
    }

    public List<DataPartitionMeta> getAllDataPartitionMetas() {
        return dataPartitions.values().stream()
                .map(DataPartitionMetaStatus::getMeta)
                .collect(Collectors.toList());
    }

    @Override
    public void onPartitionCreated(DataPartitionMeta partitionMeta) throws Exception {
        checkState(
                storagePaths.contains(partitionMeta.getStorageMeta().getStoragePath()),
                String.format(
                        "The base path %s is not configured",
                        partitionMeta.getStorageMeta().getStoragePath()));

        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(
                        partitionMeta.getDataSetID(), partitionMeta.getDataPartitionID());
        checkState(
                !dataPartitions.containsKey(coordinate), "The data partition is already exists.");

        synchronized (dataPartitions) {
            dataPartitions.put(coordinate, new DataPartitionMetaStatus(partitionMeta, false));
        }

        File storageFile = new File(partitionMeta.getStorageMeta().getStoragePath());
        File metaFile = getDataPartitionPath(storageFile, partitionMeta);

        if (!metaFile.getParentFile().exists()) {
            metaFile.getParentFile().mkdir();
        }

        try (FileOutputStream outputStream = new FileOutputStream(metaFile);
                DataOutputStream dataOutput = new DataOutputStream(outputStream)) {
            DataPartitionUtils.serializePartitionMeta(partitionMeta, dataOutput);
        }
    }

    @Override
    public void onPartitionRemoved(DataPartitionMeta partitionMeta) {
        checkState(
                storagePaths.contains(partitionMeta.getStorageMeta().getStoragePath()),
                String.format(
                        "The base path %s is not configured",
                        partitionMeta.getStorageMeta().getStoragePath()));

        File storageFile = new File(partitionMeta.getStorageMeta().getStoragePath());
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(
                        partitionMeta.getDataSetID(), partitionMeta.getDataPartitionID());
        synchronized (dataPartitions) {
            DataPartitionMetaStatus status = dataPartitions.get(coordinate);

            if (status == null) {
                LOG.warn("Data partition {} not found", coordinate);
                return;
            }

            // Marks data partition as releasing, and remove the meta after master has also marked
            // in removeReleasingDataPartition.
            dataPartitions.get(coordinate).setReleasing(true);
        }

        File metaFile = getDataPartitionPath(storageFile, partitionMeta);
        try {
            boolean deleted = metaFile.delete();
            if (!deleted) {
                LOG.warn("Unable to remove the meta file " + metaFile.getAbsolutePath());
            }
        } catch (Exception e) {
            LOG.warn("Unable to remove the meta file " + metaFile.getAbsolutePath(), e);
        }

        BiConsumer<JobID, DataPartitionCoordinate> currentRemoveConsumer = partitionRemovedConsumer;
        if (currentRemoveConsumer != null) {
            currentRemoveConsumer.accept(partitionMeta.getJobID(), coordinate);
        }
    }

    @Override
    public void removeReleasingDataPartition(DataPartitionCoordinate coordinate) {
        synchronized (dataPartitions) {
            dataPartitions.remove(coordinate);
        }
    }

    @Override
    public int getSize() {
        synchronized (dataPartitions) {
            return dataPartitions.size();
        }
    }

    private File getDataPartitionPath(File baseDir, DataPartitionMeta partitionMeta) {
        String name =
                CommonUtils.bytesToHexString(partitionMeta.getDataSetID().getId())
                        + "-"
                        + CommonUtils.bytesToHexString(partitionMeta.getDataPartitionID().getId());

        return new File(new File(baseDir, META_DIR_NAME), name);
    }

    @Override
    public void close() throws Exception {
        // TODO: would be implemented later
    }

    /** The status of one data partition on the worker side. */
    private static class DataPartitionMetaStatus {

        private final DataPartitionMeta meta;

        private boolean isReleasing;

        public DataPartitionMetaStatus(DataPartitionMeta meta, boolean isReleasing) {
            this.meta = meta;
            this.isReleasing = isReleasing;
        }

        public DataPartitionMeta getMeta() {
            return meta;
        }

        public boolean isReleasing() {
            return isReleasing;
        }

        public void setReleasing(boolean releasing) {
            isReleasing = releasing;
        }
    }
}
