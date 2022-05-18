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

package com.alibaba.flink.shuffle.client;

import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.core.ids.InstanceID;

import java.util.Set;

/** The Listener about the status change of the shuffle workers. */
public interface ShuffleWorkerStatusListener {

    /**
     * Notifies that the shuffle worker get unrelated with this job, like due to worker get
     * unavailable or all the data partitions are released.
     *
     * @param workerID the resource id of the shuffle worker.
     */
    void notifyIrrelevantWorker(InstanceID workerID);

    /**
     * Notifies that the shuffle worker get back.
     *
     * @param workerID the resource id of the shuffle worker.
     * @param dataPartitions the recovered data partitions on the shuffle worker.
     */
    void notifyRelevantWorker(InstanceID workerID, Set<DataPartitionCoordinate> dataPartitions);
}
