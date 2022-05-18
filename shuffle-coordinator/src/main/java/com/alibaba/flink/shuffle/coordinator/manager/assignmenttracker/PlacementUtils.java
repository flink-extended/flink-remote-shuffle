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

package com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker;

import static com.alibaba.flink.shuffle.core.config.StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES;
import static com.alibaba.flink.shuffle.core.config.StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES;

/** Utility methods to manipulate {@link PartitionPlacementStrategy}s. */
public class PlacementUtils {

    static void throwNoAvailableWorkerException(int numWorkers)
            throws ShuffleResourceAllocationException {
        if (numWorkers > 0) {
            throw new ShuffleResourceAllocationException(
                    String.format(
                            "No available workers. This may indicate that there is no worker node "
                                    + "with enough storage space, because maybe all workers have "
                                    + "been filtered out. You may need to decrease the configured "
                                    + "value of %s (%s by default) to allow workers with smaller "
                                    + "free storage space to be used for data writing, or you may "
                                    + "need to increase the configured value of %s (infinite by "
                                    + "default) to allow workers to use more storage space for data"
                                    + " writing.",
                            STORAGE_MIN_RESERVED_SPACE_BYTES.key(),
                            STORAGE_MIN_RESERVED_SPACE_BYTES.defaultValue().toHumanReadableString(),
                            STORAGE_MAX_USABLE_SPACE_BYTES.key()));
        } else {
            throw new ShuffleResourceAllocationException("No available workers.");
        }
    }

    static WorkerStatus[] singleElementWorkerArray(WorkerStatus workerStatus) {
        return new WorkerStatus[] {workerStatus};
    }
}
