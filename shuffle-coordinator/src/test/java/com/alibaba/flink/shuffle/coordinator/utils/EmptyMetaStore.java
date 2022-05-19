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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.worker.metastore.Metastore;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/** An empty meta store implementation. */
public class EmptyMetaStore implements Metastore {

    @Override
    public void setPartitionRemovedConsumer(
            BiConsumer<JobID, DataPartitionCoordinate> partitionRemovedConsumer) {}

    @Override
    public List<DataPartitionStatus> listDataPartitions() throws Exception {
        return new ArrayList<>();
    }

    @Override
    public void removeReleasingDataPartition(DataPartitionCoordinate dataPartitionCoordinate) {}

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public void onPartitionCreated(DataPartitionMeta partitionMeta) throws Exception {}

    @Override
    public void onPartitionRemoved(DataPartitionMeta partitionMeta) {}

    @Override
    public void close() throws Exception {}
}
