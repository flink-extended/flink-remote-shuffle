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

import com.alibaba.flink.shuffle.core.ids.RegistrationID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.singleElementWorkerArray;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.throwNoAvailableWorkerException;

/**
 * This strategy selects the next worker in random order. The available storage space of the worker
 * should be greater than the minimum configurable value.
 */
class RandomPlacementStrategy extends BasePartitionPlacementStrategy {
    private static final Random RANDOM_ORDER = new Random();

    RandomPlacementStrategy(long reservedSpaceBytes) {
        super(reservedSpaceBytes);
    }

    @Override
    public WorkerStatus[] selectNextWorker(
            Map<RegistrationID, WorkerStatus> workers,
            PartitionPlacementContext partitionPlacementContext)
            throws ShuffleResourceAllocationException {

        WorkerStatus selectedWorker = selectNextRandomWorker(workers, partitionPlacementContext);
        return singleElementWorkerArray(selectedWorker);
    }

    private WorkerStatus selectNextRandomWorker(
            Map<RegistrationID, WorkerStatus> workers,
            PartitionPlacementContext partitionPlacementContext)
            throws ShuffleResourceAllocationException {
        WorkerStatus selectedWorker = randomlySelectOneWorker(workers, partitionPlacementContext);

        if (selectedWorker == null) {
            throwNoAvailableWorkerException(workers.size());
        }

        return selectedWorker;
    }

    private WorkerStatus randomlySelectOneWorker(
            Map<RegistrationID, WorkerStatus> workers,
            PartitionPlacementContext partitionPlacementContext) {
        if (workers.isEmpty()) {
            return null;
        }

        WorkerStatus selectedWorker = null;
        List<RegistrationID> workerIDs = new ArrayList<>(workers.keySet());
        for (int startIndex = 0; startIndex < workerIDs.size(); startIndex++) {
            int selectedIndex = RANDOM_ORDER.nextInt(workerIDs.size() - startIndex) + startIndex;
            RegistrationID registrationID = workerIDs.get(selectedIndex);
            WorkerStatus currentWorker = workers.get(registrationID);
            long usableSpaceBytes =
                    PlacementUtils.getUsableSpaceBytes(
                            partitionPlacementContext.getDataPartitionFactoryName(), currentWorker);
            if (isUsableSpaceEnoughOrNotInit(usableSpaceBytes)) {
                selectedWorker = currentWorker;
                break;
            }
            swapRegistrationID(workerIDs, startIndex, selectedIndex);
        }
        return selectedWorker;
    }

    private static void swapRegistrationID(
            List<RegistrationID> workerIDs, int startIndex, int selectedIndex) {
        if (startIndex == selectedIndex) {
            return;
        }
        RegistrationID currentRegistrationID = workerIDs.get(selectedIndex);
        RegistrationID startRegistrationID = workerIDs.get(startIndex);
        workerIDs.set(startIndex, currentRegistrationID);
        workerIDs.set(selectedIndex, startRegistrationID);
    }
}
