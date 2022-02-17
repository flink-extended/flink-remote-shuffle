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

package com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker;

import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.UsableStorageSpaceInfo;

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.singleElementWorkerArray;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.throwNoAvailableWorkerException;

/**
 * This strategy selects the next worker in round robin order. The available storage space of the
 * worker should be greater than the minimum configurable value.
 */
class RoundRobinPlacementStrategy extends BasePartitionPlacementStrategy {

    RoundRobinPlacementStrategy(long reservedSpaceBytes) {
        super(reservedSpaceBytes);
    }

    @Override
    public WorkerStatus[] selectNextWorker(PartitionPlacementContext partitionPlacementContext)
            throws ShuffleResourceAllocationException {
        DataPartitionFactory partitionFactory = partitionPlacementContext.getPartitionFactory();
        int counter = 0;
        WorkerStatus selectedWorker = null;
        while (counter++ < workers.size()) {
            selectedWorker = workers.pollFirst();
            if (selectedWorker == null) {
                break;
            }
            workers.addLast(selectedWorker);
            UsableStorageSpaceInfo usableSpace =
                    selectedWorker.getStorageUsableSpace(partitionFactory.getClass().getName());
            if (isUsableSpaceEnough(partitionFactory, usableSpace)) {
                break;
            }
            selectedWorker = null;
        }

        if (selectedWorker == null) {
            throwNoAvailableWorkerException(workers.size());
        }
        return singleElementWorkerArray(selectedWorker);
    }
}
