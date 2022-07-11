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

package com.alibaba.flink.shuffle.client;

import com.alibaba.flink.shuffle.coordinator.manager.JobDataPartitionDistribution;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Client to interact with ShuffleManager to request and release shuffle resource. */
public interface ShuffleManagerClient extends AutoCloseable {

    void start();

    void synchronizeWorkerStatus(Set<InstanceID> initialWorkers) throws Exception;

    CompletableFuture<ShuffleResource> requestShuffleResource(
            DataSetID dataSetId,
            MapPartitionID mapPartitionId,
            int numberOfSubpartitions,
            long consumerGroupID,
            String dataPartitionFactoryName,
            String taskLocation);

    void releaseShuffleResource(DataSetID dataSetId, MapPartitionID mapPartitionId);

    CompletableFuture<Integer> getNumberOfRegisteredWorkers();

    CompletableFuture<Map<InstanceID, ShuffleWorkerMetrics>> getShuffleWorkerMetrics();

    CompletableFuture<List<JobID>> listJobs(boolean includeMyself);

    CompletableFuture<JobDataPartitionDistribution> getJobDataPartitionDistribution(JobID jobID);

    @Override
    void close() throws Exception;
}
