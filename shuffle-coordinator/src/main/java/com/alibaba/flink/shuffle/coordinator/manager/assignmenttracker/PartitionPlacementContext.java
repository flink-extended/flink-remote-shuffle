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

import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;

import java.util.Map;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.isMapPartition;

/**
 * A context class which is used when choose the next worker to store a new data partition. This is
 * a runtime context, while the other variables are placed in the fields of each different strategy.
 */
public class PartitionPlacementContext {

    private final Map<RegistrationID, WorkerStatus> currentWorkers;

    private final DataPartitionFactory partitionFactory;

    private final String taskLocation;

    private final int numberOfConsumers;

    PartitionPlacementContext(
            Map<RegistrationID, WorkerStatus> currentWorkers,
            DataPartitionFactory partitionFactory,
            String taskLocation,
            int numberOfConsumers) {
        checkArgument(currentWorkers != null, "Must be not null.");
        checkArgument(partitionFactory != null, "Must be not null.");
        checkArgument(numberOfConsumers > 0, "Must be positive.");

        this.currentWorkers = currentWorkers;
        this.partitionFactory = partitionFactory;
        this.taskLocation = taskLocation;
        this.numberOfConsumers = numberOfConsumers;
    }

    public Map<RegistrationID, WorkerStatus> currentWorkers() {
        return currentWorkers;
    }

    DataPartitionFactory getPartitionFactory() {
        return partitionFactory;
    }

    String getTaskLocation() {
        return taskLocation;
    }

    int numSelectWorkers() {
        return isMapPartition(partitionFactory.getDataPartitionType()) ? 1 : numberOfConsumers;
    }
}
