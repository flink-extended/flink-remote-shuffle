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

package com.alibaba.flink.shuffle.transfer.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DiskType;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

import java.io.DataInput;
import java.util.Map;

/** An empty data partition factory used for tests. */
public class NoOpMapPartitionFactory implements DataPartitionFactory {
    @Override
    public void initialize(Configuration configuration) throws Exception {}

    @Override
    public DataPartition createDataPartition(
            PartitionedDataStore dataStore,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID,
            int numMapPartitions,
            int numReducePartitions)
            throws Exception {
        return null;
    }

    @Override
    public DataPartition createDataPartition(
            PartitionedDataStore dataStore, DataPartitionMeta partitionMeta) throws Exception {
        return null;
    }

    @Override
    public DataPartitionMeta recoverDataPartitionMeta(DataInput dataInput) throws Exception {
        return null;
    }

    @Override
    public DataPartition.DataPartitionType getDataPartitionType() {
        return DataPartition.DataPartitionType.MAP_PARTITION;
    }

    @Override
    public void updateFreeStorageSpace() {}

    @Override
    public StorageSpaceInfo getStorageSpaceInfo() {
        return null;
    }

    @Override
    public boolean isStorageSpaceValid(
            StorageSpaceInfo storageSpaceInfo,
            long minReservedSpaceBytes,
            long maxUsableSpaceBytes) {
        return false;
    }

    @Override
    public String getStorageNameFromPath(String storagePath) {
        return null;
    }

    @Override
    public void updateUsedStorageSpace(Map<String, Long> storageUsedBytes) {}

    @Override
    public void updateStorageHealthStatus() {}

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
}
