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

package com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker;

import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

import java.util.LinkedList;

/** An abstract class to implement the common methods of {@link PartitionPlacementStrategy}. */
public abstract class BasePartitionPlacementStrategy implements PartitionPlacementStrategy {

    protected final long minReservedSpaceBytes;

    protected final long maxUsableSpaceBytes;

    protected final LinkedList<WorkerStatus> workers = new LinkedList<>();

    BasePartitionPlacementStrategy(long minReservedSpaceBytes, long maxUsableSpaceBytes) {
        this.minReservedSpaceBytes = minReservedSpaceBytes;
        this.maxUsableSpaceBytes = maxUsableSpaceBytes;
    }

    boolean isStorageSpaceValid(
            DataPartitionFactory partitionFactory, StorageSpaceInfo storageSpaceInfo) {
        return partitionFactory.isStorageSpaceValid(
                storageSpaceInfo, minReservedSpaceBytes, maxUsableSpaceBytes);
    }

    @Override
    public void addWorker(WorkerStatus worker) {
        workers.addLast(worker);
    }

    @Override
    public void removeWorker(WorkerStatus worker) {
        workers.remove(worker);
    }
}
