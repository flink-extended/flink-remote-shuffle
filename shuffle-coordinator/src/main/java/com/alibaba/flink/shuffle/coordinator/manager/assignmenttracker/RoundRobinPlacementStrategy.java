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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.singleElementWorkerArray;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementUtils.throwNoAvailableWorkerException;

/**
 * This strategy selects the next worker in round robin order. The available storage space of the
 * worker should be greater than the minimum configurable value.
 */
class RoundRobinPlacementStrategy extends BasePartitionPlacementStrategy {
    private final Map<RegistrationID, Long> selectWorkerCount;

    private long selectCounter;

    RoundRobinPlacementStrategy(long reservedSpaceBytes) {
        super(reservedSpaceBytes);
        this.selectWorkerCount = new HashMap<>();
        this.selectCounter = 0;
    }

    @Override
    public WorkerStatus[] selectNextWorker(
            Map<RegistrationID, WorkerStatus> workers,
            PartitionPlacementContext partitionPlacementContext)
            throws ShuffleResourceAllocationException {
        WorkerStatus selectedWorker = roundRobinSelectWorker(workers, partitionPlacementContext);

        if (selectedWorker == null) {
            throwNoAvailableWorkerException(workers.size());
        }

        return singleElementWorkerArray(selectedWorker);
    }

    private WorkerStatus roundRobinSelectWorker(
            Map<RegistrationID, WorkerStatus> workers,
            PartitionPlacementContext partitionPlacementContext) {
        WorkerStatus selectedWorker = null;

        RegistrationID chosenWorkerID = null;
        long numSelectCount = Long.MAX_VALUE;
        for (Map.Entry<RegistrationID, WorkerStatus> workerEntry : workers.entrySet()) {
            RegistrationID registrationID = workerEntry.getKey();
            WorkerStatus currentWorker = workerEntry.getValue();
            if (!selectWorkerCount.containsKey(registrationID)) {
                selectWorkerCount.put(registrationID, 0L);
            }

            long usableSpaceBytes =
                    PlacementUtils.getUsableSpaceBytes(
                            partitionPlacementContext.getDataPartitionFactoryName(),
                            workers.get(registrationID));

            if (selectWorkerCount.get(registrationID) < numSelectCount
                    && isUsableSpaceEnoughOrNotInit(usableSpaceBytes)) {
                chosenWorkerID = registrationID;
                numSelectCount = selectWorkerCount.get(registrationID);
                selectedWorker = currentWorker;
                if (selectWorkerCount.get(registrationID) == 0) {
                    break;
                }
            }
        }

        if (selectedWorker != null) {
            selectCounter++;
            selectWorkerCount.put(chosenWorkerID, selectCounter);
        }

        removeNonExistWorker(workers);
        return selectedWorker;
    }

    private void removeNonExistWorker(Map<RegistrationID, WorkerStatus> workers) {
        Set<RegistrationID> toRemoveWorkerIDs = new HashSet<>();
        for (RegistrationID registrationID : selectWorkerCount.keySet()) {
            if (!workers.containsKey(registrationID)) {
                toRemoveWorkerIDs.add(registrationID);
            }
        }
        toRemoveWorkerIDs.forEach(selectWorkerCount::remove);
    }
}
