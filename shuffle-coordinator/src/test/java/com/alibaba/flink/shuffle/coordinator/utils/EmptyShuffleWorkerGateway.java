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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;

import java.util.concurrent.CompletableFuture;

/** A test empty shuffle worker gateway implementation. */
public class EmptyShuffleWorkerGateway implements ShuffleWorkerGateway {

    @Override
    public void heartbeatFromManager(InstanceID managerID) {}

    @Override
    public void disconnectManager(Exception cause) {}

    @Override
    public CompletableFuture<Acknowledge> releaseDataPartition(
            JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> removeReleasedDataPartitionMeta(
            JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<ShuffleWorkerMetrics> getWorkerMetrics() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getAddress() {
        return "";
    }

    @Override
    public String getHostname() {
        return "";
    }
}
