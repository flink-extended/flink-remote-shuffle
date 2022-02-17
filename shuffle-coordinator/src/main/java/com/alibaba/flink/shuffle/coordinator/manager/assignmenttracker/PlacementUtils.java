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

import static com.alibaba.flink.shuffle.core.config.StorageOptions.STORAGE_RESERVED_SPACE_BYTES;

/** Utility methods to manipulate {@link PartitionPlacementStrategy}s. */
public class PlacementUtils {

    static void throwNoAvailableWorkerException(int numWorkers)
            throws ShuffleResourceAllocationException {
        if (numWorkers > 0) {
            throw new ShuffleResourceAllocationException(
                    "No available workers. This may not indicate that there is no normal worker node,"
                            + " because maybe all workers have been filtered out."
                            + " You can decrease the value of the configuration "
                            + STORAGE_RESERVED_SPACE_BYTES.key()
                            + " ("
                            + STORAGE_RESERVED_SPACE_BYTES.defaultValue().toHumanReadableString()
                            + " by default) to allow workers with smaller storage space to be used"
                            + " for writing data.");
        } else {
            throw new ShuffleResourceAllocationException("No available workers");
        }
    }

    static WorkerStatus[] singleElementWorkerArray(WorkerStatus workerStatus) {
        return new WorkerStatus[] {workerStatus};
    }
}
