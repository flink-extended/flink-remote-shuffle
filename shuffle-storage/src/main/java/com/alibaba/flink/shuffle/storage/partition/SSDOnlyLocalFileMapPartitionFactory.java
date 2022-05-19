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
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.StorageType;

import java.util.List;

/**
 * A {@link LocalFileMapPartitionFactory} variant which only uses SSD to store data partition data.
 */
public class SSDOnlyLocalFileMapPartitionFactory extends LocalFileMapPartitionFactory {

    @Override
    public void initialize(Configuration configuration) {
        super.initialize(configuration);

        if (ssdStorageMetas.isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "No valid data dir of SSD storage type is configured for %s.",
                            StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
        }
        hddStorageMetas.clear();
        updateStorageHealthStatus();
        updateFreeStorageSpace();
    }

    @Override
    protected StorageMeta getNextDataStorageMeta() {
        synchronized (lock) {
            return getStorageMetaInNonEmptyQueue(ssdStorageMetas);
        }
    }

    @Override
    public void updateFreeStorageSpace() {
        storageSpaceInfo.setHddMaxFreeSpaceBytes(0);
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
    }

    @Override
    public boolean isStorageSpaceValid(
            StorageSpaceInfo storageSpaceInfo,
            long minReservedSpaceBytes,
            long maxUsableSpaceBytes) {
        return minReservedSpaceBytes < storageSpaceInfo.getSsdMaxFreeSpaceBytes()
                && maxUsableSpaceBytes > storageSpaceInfo.getSsdMaxUsedSpaceBytes();
    }

    @Override
    public void updateStorageHealthStatus() {
        List<StorageMeta> storageMetas = getSsdStorageMetas();
        for (StorageMeta storageMeta : storageMetas) {
            storageMeta.updateStorageHealthStatus();
        }
    }

    @Override
    public boolean useSsdOnly() {
        return true;
    }

    @Override
    public StorageType getPreferredStorageType() {
        return StorageType.SSD;
    }
}
