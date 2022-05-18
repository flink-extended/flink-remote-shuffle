/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

package com.alibaba.flink.shuffle.coordinator.worker.metastore;

import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.listener.PartitionStateListener;

import java.util.List;
import java.util.function.BiConsumer;

/** The interface for metastore. */
public interface Metastore extends PartitionStateListener, AutoCloseable {

    void setPartitionRemovedConsumer(
            BiConsumer<JobID, DataPartitionCoordinate> partitionRemovedConsumer);

    /**
     * Lists the data partitions stored in the meta store.
     *
     * @return The list of data partitions according to the meta store.
     */
    List<DataPartitionStatus> listDataPartitions() throws Exception;

    /**
     * Remove the releasing data partition after master has marked as released.
     *
     * @param dataPartitionCoordinate The coordinate of data partition to remove.
     */
    void removeReleasingDataPartition(DataPartitionCoordinate dataPartitionCoordinate);

    int getSize();
}
