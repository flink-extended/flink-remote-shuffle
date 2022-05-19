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

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.singleElementWorkerArray;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.throwNoAvailableWorkerException;

/**
 * This strategy will select the next worker with two conditions. The first is that the number of
 * data partitions stored by the worker is the smallest. The second is that the available storage
 * space of the worker is greater than the minimum configurable value.
 */
class MinNumberPlacementStrategy extends BasePartitionPlacementStrategy {

    MinNumberPlacementStrategy(long minReservedSpaceBytes, long maxUsableSpaceBytes) {
        super(minReservedSpaceBytes, maxUsableSpaceBytes);
    }

    @Override
    public WorkerStatus[] selectNextWorker(PartitionPlacementContext partitionPlacementContext)
            throws ShuffleResourceAllocationException {
        DataPartitionFactory partitionFactory = partitionPlacementContext.getPartitionFactory();
        WorkerStatus selectedWorker = null;
        for (WorkerStatus workerStatus : workers) {
            StorageSpaceInfo storageSpaceInfo =
                    workerStatus.getStorageSpaceInfo(partitionFactory.getClass().getName());
            if (isStorageSpaceValid(partitionFactory, storageSpaceInfo)) {
                if (selectedWorker == null
                        || workerStatus.getDataPartitions().size()
                                < selectedWorker.getDataPartitions().size()) {
                    selectedWorker = workerStatus;
                }
            }
        }

        if (selectedWorker == null) {
            throwNoAvailableWorkerException(workers.size());
        }
        return singleElementWorkerArray(selectedWorker);
    }
}
