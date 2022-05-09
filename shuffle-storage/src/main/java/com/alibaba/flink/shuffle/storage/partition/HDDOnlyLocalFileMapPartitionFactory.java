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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.StorageType;

import java.util.List;

/**
 * A {@link LocalFileMapPartitionFactory} variant which only uses HDD to store data partition data.
 */
public class HDDOnlyLocalFileMapPartitionFactory extends LocalFileMapPartitionFactory {

    @Override
    public void initialize(Configuration configuration) {
        super.initialize(configuration);

        if (hddStorageMetas.isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "No valid data dir of HDD storage type is configured for %s.",
                            StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
        }
        ssdStorageMetas.clear();
        updateStorageHealthStatus();
        this.updateFreeStorageSpace();
    }

    @Override
    protected StorageMeta getNextDataStorageMeta() {
        synchronized (lock) {
            return getStorageMetaInNonEmptyQueue(hddStorageMetas);
        }
    }

    @Override
    public DataPartition.DataPartitionType getDataPartitionType() {
        return super.getDataPartitionType();
    }

    @Override
    public void updateFreeStorageSpace() {
        storageSpaceInfo.setSsdMaxFreeSpaceBytes(0);
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
    public boolean isStorageSpaceValid(
            StorageSpaceInfo storageSpaceInfo,
            long minReservedSpaceBytes,
            long maxUsableSpaceBytes) {
        return minReservedSpaceBytes < storageSpaceInfo.getHddMaxFreeSpaceBytes()
                && maxUsableSpaceBytes > storageSpaceInfo.getHddMaxUsedSpaceBytes();
    }

    @Override
    public void updateStorageHealthStatus() {
        List<StorageMeta> storageMetas = getHddStorageMetas();
        for (StorageMeta storageMeta : storageMetas) {
            storageMeta.updateStorageHealthStatus();
        }
    }

    @Override
    public boolean useHddOnly() {
        return true;
    }

    @Override
    public StorageType getPreferredStorageType() {
        return StorageType.HDD;
    }
}
