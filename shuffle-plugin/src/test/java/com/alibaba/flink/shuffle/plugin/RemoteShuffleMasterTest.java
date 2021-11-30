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

package com.alibaba.flink.shuffle.plugin;

import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;
import com.alibaba.flink.shuffle.plugin.utils.ConfigurationUtils;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/** Tests the behavior of {@link RemoteShuffleMaster}. */
public class RemoteShuffleMasterTest extends RemoteShuffleShuffleTestBase {

    @Test
    public void testResourceAllocateAndRelease() throws Exception {
        // Resource request
        CompletableFuture<Tuple4<JobID, DataSetID, MapPartitionID, Integer>> resourceRequestFuture =
                new CompletableFuture<>();
        ShuffleResource shuffleResource =
                new DefaultShuffleResource(
                        new ShuffleWorkerDescriptor[] {
                            new ShuffleWorkerDescriptor(new InstanceID("worker1"), "worker1", 20480)
                        },
                        DataPartition.DataPartitionType.MAP_PARTITION);
        smGateway.setAllocateShuffleResourceConsumer(
                (jobID, dataSetID, mapPartitionID, numberOfSubpartitions) -> {
                    resourceRequestFuture.complete(
                            new Tuple4<>(jobID, dataSetID, mapPartitionID, numberOfSubpartitions));
                    return CompletableFuture.completedFuture(shuffleResource);
                });

        // Resource release
        CompletableFuture<Tuple3<JobID, DataSetID, MapPartitionID>> resourceReleaseFuture =
                new CompletableFuture<>();
        smGateway.setReleaseShuffleResourceConsumer(
                (jobID, dataSetID, mapPartitionID) -> {
                    resourceReleaseFuture.complete(new Tuple3<>(jobID, dataSetID, mapPartitionID));
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        org.apache.flink.api.common.JobID jobID = new org.apache.flink.api.common.JobID();
        IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();
        IntermediateResultPartitionID intermediateResultPartitionId =
                new IntermediateResultPartitionID(intermediateDataSetId, 0);
        ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
        ResultPartitionID resultPartitionId =
                new ResultPartitionID(intermediateResultPartitionId, executionAttemptId);
        PartitionDescriptor partitionDescriptor =
                new PartitionDescriptor(
                        intermediateDataSetId,
                        10,
                        intermediateResultPartitionId,
                        ResultPartitionType.BLOCKING,
                        5,
                        1);
        ProducerDescriptor producerDescriptor =
                new ProducerDescriptor(
                        new ResourceID("tm1"),
                        executionAttemptId,
                        InetAddress.getLocalHost(),
                        50000);
        try (RemoteShuffleMaster shuffleMaster = createAndInitializeShuffleMaster(jobID)) {
            CompletableFuture<RemoteShuffleDescriptor> shuffleDescriptorFuture =
                    shuffleMaster.registerPartitionWithProducer(
                            jobID, partitionDescriptor, producerDescriptor);
            shuffleDescriptorFuture.join();
            RemoteShuffleDescriptor shuffleDescriptor = shuffleDescriptorFuture.get();
            assertEquals(resultPartitionId, shuffleDescriptor.getResultPartitionID());
            Assert.assertEquals(shuffleResource, shuffleDescriptor.getShuffleResource());

            shuffleMaster.releasePartitionExternally(shuffleDescriptor);
            assertEquals(
                    new Tuple3<>(
                            shuffleDescriptor.getJobId(),
                            shuffleDescriptor.getDataSetId(),
                            shuffleDescriptor.getDataPartitionID()),
                    resourceReleaseFuture.get(TIMEOUT, TimeUnit.MILLISECONDS));
        }
    }

    @Test
    public void testShuffleMemoryAnnouncing() throws Exception {
        try (RemoteShuffleMaster shuffleMaster =
                createAndInitializeShuffleMaster(new org.apache.flink.api.common.JobID())) {
            Map<IntermediateDataSetID, Integer> numberOfInputGateChannels = new HashMap<>();
            Map<IntermediateDataSetID, Integer> numbersOfResultSubpartitions = new HashMap<>();
            Map<IntermediateDataSetID, ResultPartitionType> resultPartitionTypes = new HashMap<>();
            IntermediateDataSetID inputDataSetID0 = new IntermediateDataSetID();
            IntermediateDataSetID inputDataSetID1 = new IntermediateDataSetID();
            IntermediateDataSetID outputDataSetID0 = new IntermediateDataSetID();
            IntermediateDataSetID outputDataSetID1 = new IntermediateDataSetID();
            IntermediateDataSetID outputDataSetID2 = new IntermediateDataSetID();
            Random random = new Random();
            numberOfInputGateChannels.put(inputDataSetID0, random.nextInt(1000));
            numberOfInputGateChannels.put(inputDataSetID1, random.nextInt(1000));
            numbersOfResultSubpartitions.put(outputDataSetID0, random.nextInt(1000));
            numbersOfResultSubpartitions.put(outputDataSetID1, random.nextInt(1000));
            numbersOfResultSubpartitions.put(outputDataSetID2, random.nextInt(1000));
            resultPartitionTypes.put(outputDataSetID0, ResultPartitionType.BLOCKING);
            resultPartitionTypes.put(outputDataSetID1, ResultPartitionType.BLOCKING);
            resultPartitionTypes.put(outputDataSetID2, ResultPartitionType.BLOCKING);
            MemorySize calculated =
                    shuffleMaster.computeShuffleMemorySizeForTask(
                            TaskInputsOutputsDescriptor.from(
                                    numberOfInputGateChannels,
                                    numbersOfResultSubpartitions,
                                    resultPartitionTypes));

            long numBytesPerGate =
                    configuration.getMemorySize(PluginOptions.MEMORY_PER_INPUT_GATE).getBytes();
            long expectedInput = 2 * numBytesPerGate;

            long numBytesPerResultPartition =
                    configuration
                            .getMemorySize(PluginOptions.MEMORY_PER_RESULT_PARTITION)
                            .getBytes();
            long expectedOutput = 3 * numBytesPerResultPartition;
            MemorySize expected = new MemorySize(expectedInput + expectedOutput);

            assertEquals(expected, calculated);
        }
    }

    private RemoteShuffleMaster createAndInitializeShuffleMaster(
            org.apache.flink.api.common.JobID jobID) throws Exception {
        RemoteShuffleMaster shuffleMaster =
                new RemoteShuffleMaster(
                        new ShuffleMasterContext() {
                            @Override
                            public Configuration getConfiguration() {
                                return ConfigurationUtils.toFlinkConfiguration(configuration);
                            }

                            @Override
                            public void onFatalError(Throwable throwable) {
                                System.exit(-100);
                            }
                        }) {
                    @Override
                    protected RemoteShuffleRpcService createRpcService() throws Exception {
                        return rpcService;
                    }
                };

        shuffleMaster.start();
        shuffleMaster.registerJob(
                new JobShuffleContext() {
                    @Override
                    public org.apache.flink.api.common.JobID getJobId() {
                        return jobID;
                    }

                    @Override
                    public CompletableFuture<?> stopTrackingAndReleasePartitions(
                            Collection<ResultPartitionID> collection) {
                        return CompletableFuture.completedFuture(null);
                    }
                });

        return shuffleMaster;
    }
}
