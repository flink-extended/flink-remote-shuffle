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

import com.alibaba.flink.shuffle.client.ShuffleManagerClient;
import com.alibaba.flink.shuffle.client.ShuffleManagerClientConfiguration;
import com.alibaba.flink.shuffle.client.ShuffleManagerClientImpl;
import com.alibaba.flink.shuffle.client.ShuffleWorkerStatusListener;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServicesUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServiceUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;
import com.alibaba.flink.shuffle.plugin.utils.ConfigurationUtils;
import com.alibaba.flink.shuffle.plugin.utils.IdMappingUtils;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.JobShuffleContext;
import org.apache.flink.runtime.shuffle.PartitionDescriptor;
import org.apache.flink.runtime.shuffle.ProducerDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.TaskInputsOutputsDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** The shuffle manager implementation for remote shuffle service plugin. */
public class RemoteShuffleMaster implements ShuffleMaster<RemoteShuffleDescriptor> {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleMaster.class);

    private static final int MAX_RETRY_TIMES = 3;

    private final ShuffleMasterContext shuffleMasterContext;

    private final Configuration configuration;

    // Job level configuration will be supported in the future
    private final String partitionFactory;

    private final Duration workerRecoverTimeout;

    private final Map<JobID, ShuffleClient> shuffleClients = new HashMap<>();

    private final ScheduledThreadPoolExecutor executor =
            new ScheduledThreadPoolExecutor(
                    1, runnable -> new Thread(runnable, "remote-shuffle-master-executor"));

    private final RemoteShuffleRpcService rpcService;

    private final HaServices haServices;

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public RemoteShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
        CommonUtils.checkArgument(shuffleMasterContext != null, "Must be not null.");

        this.shuffleMasterContext = shuffleMasterContext;
        this.executor.setRemoveOnCancelPolicy(true);
        this.configuration =
                ConfigurationUtils.fromFlinkConfiguration(shuffleMasterContext.getConfiguration());
        this.partitionFactory = configuration.getString(PluginOptions.DATA_PARTITION_FACTORY_NAME);
        this.workerRecoverTimeout =
                configuration.getDuration(WorkerOptions.MAX_WORKER_RECOVER_TIME);

        Throwable error = null;
        RemoteShuffleRpcService tmpRpcService = null;
        try {
            tmpRpcService = createRpcService();
        } catch (Throwable throwable) {
            LOG.error("Failed to create the shuffle master RPC service.", throwable);
            error = throwable;
        }
        this.rpcService = tmpRpcService;

        HaServices tmpHAService = null;
        try {
            tmpHAService = HaServiceUtils.createHAServices(configuration);
        } catch (Throwable throwable) {
            LOG.error("Failed to create the shuffle master HA service.", throwable);
            error = throwable;
        }
        this.haServices = tmpHAService;

        if (error != null) {
            close();
            shuffleMasterContext.onFatalError(error);
            throw new ShuffleException("Failed to initialize shuffle master.", error);
        }
    }

    @Override
    public CompletableFuture<RemoteShuffleDescriptor> registerPartitionWithProducer(
            org.apache.flink.api.common.JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {
        CompletableFuture<RemoteShuffleDescriptor> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    try {
                        CommonUtils.checkState(!isClosed.get(), "ShuffleMaster has been closed.");
                        JobID shuffleJobID = IdMappingUtils.fromFlinkJobId(jobID);
                        ShuffleClient shuffleClient =
                                CommonUtils.checkNotNull(shuffleClients.get(shuffleJobID));

                        ResultPartitionID resultPartitionID =
                                new ResultPartitionID(
                                        partitionDescriptor.getPartitionId(),
                                        producerDescriptor.getProducerExecutionId());
                        DataSetID dataSetID =
                                IdMappingUtils.fromFlinkDataSetId(
                                        partitionDescriptor.getResultId());
                        MapPartitionID mapPartitionId =
                                IdMappingUtils.fromFlinkResultPartitionID(resultPartitionID);

                        shuffleClient
                                .getClient()
                                .requestShuffleResource(
                                        dataSetID,
                                        mapPartitionId,
                                        partitionDescriptor.getNumberOfSubpartitions(),
                                        partitionFactory,
                                        producerDescriptor.getAddress().getHostName())
                                .whenComplete(
                                        (shuffleResource, throwable) -> {
                                            if (throwable != null) {
                                                future.completeExceptionally(throwable);
                                                return;
                                            }
                                            InstanceID workerID =
                                                    shuffleResource
                                                            .getMapPartitionLocation()
                                                            .getWorkerId();
                                            future.complete(
                                                    new RemoteShuffleDescriptor(
                                                            resultPartitionID,
                                                            shuffleJobID,
                                                            shuffleResource));
                                            shuffleClient
                                                    .getListener()
                                                    .addPartition(workerID, resultPartitionID);
                                        });
                    } catch (Throwable throwable) {
                        LOG.error("Failed to allocate shuffle resource.", throwable);
                        future.completeExceptionally(throwable);
                    }
                });
        return future;
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
        executor.execute(
                () -> {
                    if (!(shuffleDescriptor instanceof RemoteShuffleDescriptor)) {
                        LOG.error(
                                "Only RemoteShuffleDescriptor is supported {}.",
                                shuffleDescriptor.getClass().getName());
                        shuffleMasterContext.onFatalError(
                                new ShuffleException("Illegal shuffle descriptor type."));
                        return;
                    }

                    RemoteShuffleDescriptor descriptor =
                            (RemoteShuffleDescriptor) shuffleDescriptor;
                    try {
                        ShuffleClient shuffleClient = shuffleClients.get(descriptor.getJobId());
                        if (shuffleClient != null) {
                            shuffleClient
                                    .getClient()
                                    .releaseShuffleResource(
                                            descriptor.getDataSetId(),
                                            (MapPartitionID) descriptor.getDataPartitionID());
                        }
                    } catch (Throwable throwable) {
                        // it is not a problem if we failed to release the target data partition
                        // because the session timeout mechanism will do the work for us latter
                        LOG.debug("Failed to release data partition {}.", descriptor, throwable);
                    }
                });
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            executor.execute(
                    () -> {
                        for (ShuffleClient clientWithListener : shuffleClients.values()) {
                            try {
                                clientWithListener.close();
                            } catch (Throwable throwable) {
                                LOG.error("Failed to close shuffle client.", throwable);
                            }
                        }
                        shuffleClients.clear();

                        try {
                            if (haServices != null) {
                                haServices.close();
                            }
                        } catch (Throwable throwable) {
                            LOG.error("Failed to close HA service.", throwable);
                        }

                        try {
                            if (rpcService != null) {
                                rpcService.stopService().get();
                            }
                        } catch (Throwable throwable) {
                            LOG.error("Failed to close the rpc service.", throwable);
                        }

                        try {
                            executor.shutdown();
                        } catch (Throwable throwable) {
                            LOG.error("Failed to close the shuffle master executor.", throwable);
                        }
                    });
        }
    }

    @Override
    public void registerJob(JobShuffleContext context) {
        CompletableFuture<?> future = new CompletableFuture<>();
        executor.execute(
                () -> {
                    JobID jobID = IdMappingUtils.fromFlinkJobId(context.getJobId());
                    if (shuffleClients.containsKey(jobID)) {
                        future.completeExceptionally(
                                new ShuffleException("Duplicated job registration."));
                        LOG.error("Duplicated job registration {}:{}.", context.getJobId(), jobID);
                        return;
                    }

                    try {
                        LOG.info("Registering job {}:{}", context.getJobId(), jobID);
                        CommonUtils.checkState(!isClosed.get(), "ShuffleMaster has been closed.");

                        ShuffleManagerClientConfiguration shuffleManagerClientConfiguration =
                                ShuffleManagerClientConfiguration.fromConfiguration(configuration);

                        HeartbeatServices heartbeatServices =
                                HeartbeatServicesUtils.createManagerJobHeartbeatServices(
                                        configuration);
                        ShuffleWorkerStatusListenerImpl listener =
                                new ShuffleWorkerStatusListenerImpl(context);
                        ShuffleManagerClient client =
                                new ShuffleManagerClientImpl(
                                        jobID,
                                        listener,
                                        rpcService,
                                        shuffleMasterContext::onFatalError,
                                        shuffleManagerClientConfiguration,
                                        haServices,
                                        heartbeatServices);
                        shuffleClients.put(jobID, new ShuffleClient(client, listener));
                        client.start();
                        future.complete(null);
                    } catch (Throwable throwable) {
                        LOG.error("Failed to register job.", throwable);
                        future.completeExceptionally(throwable);
                        CommonUtils.runQuietly(() -> unregisterJob(context.getJobId()));
                    }
                });
        try {
            future.get();
        } catch (InterruptedException | ExecutionException exception) {
            ExceptionUtils.rethrowAsRuntimeException(exception);
        }
    }

    @Override
    public void unregisterJob(org.apache.flink.api.common.JobID flinkJobID) {
        executor.execute(
                () -> {
                    try {
                        JobID jobID = IdMappingUtils.fromFlinkJobId(flinkJobID);
                        LOG.info("Unregister job {}:{}", flinkJobID, jobID);
                        ShuffleClient clientWithListener = shuffleClients.remove(jobID);
                        if (clientWithListener != null) {
                            clientWithListener.close();
                        }
                    } catch (Throwable throwable) {
                        LOG.error(
                                "Encounter an error when unregistering job {}:{}.",
                                flinkJobID,
                                IdMappingUtils.fromFlinkJobId(flinkJobID),
                                throwable);
                    }
                });
    }

    RemoteShuffleRpcService createRpcService() throws Exception {
        org.apache.flink.configuration.Configuration configuration =
                new org.apache.flink.configuration.Configuration(
                        shuffleMasterContext.getConfiguration());
        configuration.set(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN, 2);
        configuration.set(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX, 2);
        configuration.set(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR, 1.0);

        AkkaRpcServiceUtils.AkkaRpcServiceBuilder rpcServiceBuilder =
                AkkaRpcServiceUtils.remoteServiceBuilder(
                        ConfigurationUtils.fromFlinkConfiguration(configuration), null, "0");
        return rpcServiceBuilder.withBindAddress("0.0.0.0").createAndStart();
    }

    @Override
    public MemorySize computeShuffleMemorySizeForTask(
            TaskInputsOutputsDescriptor taskInputsOutputsDescriptor) {
        for (ResultPartitionType partitionType :
                taskInputsOutputsDescriptor.getPartitionTypes().values()) {
            if (!partitionType.isBlocking()) {
                throw new ShuffleException(
                        "Blocking result partition type expected but found " + partitionType);
            }
        }

        int numResultPartitions = taskInputsOutputsDescriptor.getSubpartitionNums().size();
        long numBytesPerPartition =
                configuration.getMemorySize(PluginOptions.MEMORY_PER_RESULT_PARTITION).getBytes();
        long numBytesForOutput = numBytesPerPartition * numResultPartitions;

        int numInputGates = taskInputsOutputsDescriptor.getInputChannelNums().size();
        long numBytesPerGate =
                configuration.getMemorySize(PluginOptions.MEMORY_PER_INPUT_GATE).getBytes();
        long numBytesForInput = numBytesPerGate * numInputGates;

        LOG.debug(
                "Announcing number of bytes {} for output and {} for input.",
                numBytesForOutput,
                numBytesForInput);

        return new MemorySize(numBytesForInput + numBytesForOutput);
    }

    private class ShuffleWorkerStatusListenerImpl implements ShuffleWorkerStatusListener {

        private final JobShuffleContext context;

        private final Map<InstanceID, Set<ResultPartitionID>> partitions = new HashMap<>();

        private final Map<InstanceID, ScheduledFuture<?>> problematicWorkers = new HashMap<>();

        ShuffleWorkerStatusListenerImpl(JobShuffleContext context) {
            CommonUtils.checkArgument(context != null, "Must be not null.");

            this.context = context;
        }

        private void addPartition(InstanceID workerID, ResultPartitionID partitionID) {
            Set<ResultPartitionID> ids =
                    partitions.computeIfAbsent(workerID, (id) -> new HashSet<>());
            ids.add(partitionID);
            ScheduledFuture<?> scheduledFuture = problematicWorkers.remove(workerID);
            if (scheduledFuture != null) {
                scheduledFuture.cancel(false);
            }
        }

        @Override
        public void notifyIrrelevantWorker(InstanceID workerID) {
            executor.execute(
                    () -> {
                        if (!problematicWorkers.containsKey(workerID)) {
                            ScheduledFuture<?> scheduledFuture =
                                    executor.schedule(
                                            () -> {
                                                Set<ResultPartitionID> partitionIDS =
                                                        partitions.remove(workerID);
                                                problematicWorkers.remove(workerID);
                                                stopTrackingPartitions(
                                                        partitionIDS,
                                                        new AtomicInteger(MAX_RETRY_TIMES));
                                            },
                                            workerRecoverTimeout.getSeconds(),
                                            TimeUnit.SECONDS);
                            problematicWorkers.put(workerID, scheduledFuture);
                        }
                    });
        }

        @Override
        public void notifyRelevantWorker(
                InstanceID workerID, Set<DataPartitionCoordinate> dataPartitions) {
            Set<ResultPartitionID> partitionIDs = new HashSet<>();
            for (DataPartitionCoordinate coordinate : dataPartitions) {
                partitionIDs.add(
                        IdMappingUtils.fromMapPartitionID(
                                (MapPartitionID) coordinate.getDataPartitionId()));
            }

            if (partitionIDs.isEmpty()) {
                return;
            }

            if (partitions.containsKey(workerID)) {
                executor.execute(
                        () -> {
                            cancelScheduledFuture(problematicWorkers.remove(workerID));
                            Set<ResultPartitionID> trackedPartitions = partitions.get(workerID);
                            partitions.put(workerID, partitionIDs);
                            for (ResultPartitionID partitionID : partitionIDs) {
                                trackedPartitions.remove(partitionID);
                            }
                            stopTrackingPartitions(
                                    trackedPartitions, new AtomicInteger(MAX_RETRY_TIMES));
                        });
            } else {
                executor.execute(
                        () -> {
                            InstanceID oldWorkerID = null;
                            ResultPartitionID targetPartitionID = partitionIDs.iterator().next();
                            for (InstanceID candidate : problematicWorkers.keySet()) {
                                Set<ResultPartitionID> idSet = partitions.get(candidate);
                                if (idSet != null && idSet.contains(targetPartitionID)) {
                                    oldWorkerID = candidate;
                                    break;
                                }
                            }

                            if (oldWorkerID != null) {
                                Set<ResultPartitionID> idSet = partitions.get(oldWorkerID);
                                for (ResultPartitionID partitionID : partitionIDs) {
                                    idSet.remove(partitionID);
                                }
                                if (idSet.isEmpty()) {
                                    partitions.remove(oldWorkerID);
                                }
                                partitions.put(workerID, partitionIDs);
                            }
                        });
            }
        }

        private void stopTrackingPartitions(
                Set<ResultPartitionID> partitionIDS, AtomicInteger remainingRetries) {
            if (partitionIDS == null || partitionIDS.isEmpty()) {
                return;
            }

            int count = remainingRetries.decrementAndGet();
            try {
                CompletableFuture<?> future =
                        context.stopTrackingAndReleasePartitions(partitionIDS);
                future.whenCompleteAsync(
                        (ignored, throwable) -> {
                            if (throwable == null) {
                                return;
                            }

                            if (count == 0) {
                                LOG.error(
                                        "Failed to stop tracking partitions {}.",
                                        Arrays.toString(partitionIDS.toArray()));
                                return;
                            }
                            stopTrackingPartitions(partitionIDS, remainingRetries);
                        },
                        executor);
            } catch (Throwable throwable) {
                if (count == 0) {
                    LOG.error(
                            "Failed to stop tracking partitions {}.",
                            Arrays.toString(partitionIDS.toArray()));
                    return;
                }
                stopTrackingPartitions(partitionIDS, remainingRetries);
            }
        }

        public JobShuffleContext getContext() {
            return context;
        }

        public Map<InstanceID, Set<ResultPartitionID>> getPartitions() {
            return partitions;
        }

        public Map<InstanceID, ScheduledFuture<?>> getProblematicWorkers() {
            return problematicWorkers;
        }
    }

    private static void cancelScheduledFuture(ScheduledFuture<?> scheduledFuture) {
        try {
            if (scheduledFuture != null && !scheduledFuture.cancel(false)) {
                LOG.error("Failed to cancel the scheduled future, may already run.");
            }
        } catch (Throwable throwable) {
            LOG.error("Error encountered when cancel the scheduled future.", throwable);
            throw throwable;
        }
    }

    private static class ShuffleClient implements AutoCloseable {

        private final ShuffleManagerClient client;

        private final ShuffleWorkerStatusListenerImpl listener;

        ShuffleClient(ShuffleManagerClient client, ShuffleWorkerStatusListenerImpl listener) {
            CommonUtils.checkArgument(client != null, "Must be not null.");
            CommonUtils.checkArgument(listener != null, "Must be not null.");

            this.client = client;
            this.listener = listener;
        }

        @Override
        public void close() throws Exception {
            Throwable error = null;

            for (Set<ResultPartitionID> ids : listener.getPartitions().values()) {
                try {
                    listener.getContext().stopTrackingAndReleasePartitions(ids);
                } catch (Throwable throwable) {
                    error = error == null ? throwable : error;
                    LOG.error(
                            "Failed to stop tracking partitions {}.",
                            Arrays.toString(ids.toArray()),
                            throwable);
                }
            }
            listener.getPartitions().clear();

            for (ScheduledFuture<?> scheduledFuture : listener.getProblematicWorkers().values()) {
                try {
                    cancelScheduledFuture(scheduledFuture);
                } catch (Throwable throwable) {
                    error = error == null ? throwable : error;
                    LOG.error("Failed to cancel scheduled future.", throwable);
                }
            }
            listener.getProblematicWorkers().clear();

            try {
                client.close();
            } catch (Throwable throwable) {
                error = error == null ? throwable : error;
                LOG.error("Failed to close shuffle client.", throwable);
            }

            if (error != null) {
                ExceptionUtils.rethrowException(error);
            }
        }

        public ShuffleManagerClient getClient() {
            return client;
        }

        public ShuffleWorkerStatusListenerImpl getListener() {
            return listener;
        }
    }
}
