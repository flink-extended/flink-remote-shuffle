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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.TestingHaServices;
import com.alibaba.flink.shuffle.coordinator.leaderretrieval.SettableLeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ManagerToJobHeartbeatPayload;
import com.alibaba.flink.shuffle.coordinator.manager.RegistrationSuccess;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.ChangedWorkerStatus;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.coordinator.utils.RecordingHeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.utils.TestingFatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.utils.TestingShuffleManagerGateway;
import com.alibaba.flink.shuffle.core.config.RpcOptions;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DiskType;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;
import com.alibaba.flink.shuffle.rpc.test.TestingRpcService;
import com.alibaba.flink.shuffle.rpc.utils.RpcUtils;
import com.alibaba.flink.shuffle.utils.Tuple4;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the behaviour of {@link ShuffleManagerClientImpl}. */
public class ShuffleManagerClientTest {

    private static final String partitionFactoryName =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory";

    private static final long timeout = 60000L;

    private ExecutorService jobMainThreadExecutor;

    private TestingRpcService rpcService;

    private Configuration configuration;

    private SettableLeaderRetrievalService shuffleManagerLeaderRetrieveService;

    private TestingHaServices haServices;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    @Before
    public void setup() throws IOException {
        jobMainThreadExecutor = Executors.newSingleThreadExecutor();

        rpcService = new TestingRpcService();

        configuration = new Configuration();

        shuffleManagerLeaderRetrieveService = new SettableLeaderRetrievalService();
        haServices = new TestingHaServices();
        haServices.setShuffleManagerLeaderRetrieveService(shuffleManagerLeaderRetrieveService);

        testingFatalErrorHandler = new TestingFatalErrorHandler();
    }

    @After
    public void teardown() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService, timeout);
            rpcService = null;
        }

        if (jobMainThreadExecutor != null) {
            jobMainThreadExecutor.shutdownNow();
        }
    }

    @Test
    public void testClientRegisterAndHeartbeat() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        // Registration future
        CompletableFuture<JobID> clientRegistrationFuture = new CompletableFuture<>();
        RegistrationSuccess registrationSuccess =
                new RegistrationSuccess(smGateway.getInstanceID());
        smGateway.setRegisterClientConsumer(
                jobID -> {
                    clientRegistrationFuture.complete(jobID);
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        // heartbeat future
        CompletableFuture<JobID> heartbeatFuture = new CompletableFuture<>();
        List<InstanceID> unrelatedShuffleWorkers =
                Collections.singletonList(new InstanceID("worker1"));
        Map<InstanceID, Set<DataPartitionCoordinate>> relatedShuffleWorkers =
                Collections.singletonMap(
                        new InstanceID("worker2"),
                        Collections.singleton(
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId())));

        smGateway.setHeartbeatFromClientConsumer(
                (jobId, cachedWorkerList) -> {
                    heartbeatFuture.complete(jobId);
                    return CompletableFuture.completedFuture(
                            new ManagerToJobHeartbeatPayload(
                                    smGateway.getInstanceID(),
                                    new ChangedWorkerStatus(
                                            unrelatedShuffleWorkers, relatedShuffleWorkers)));
                });
        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        RecordShuffleWorkerStatusListener shuffleWorkerStatusListener =
                new RecordShuffleWorkerStatusListener();

        JobID jobId = RandomIDUtils.randomJobId();
        try (ShuffleManagerClientImpl shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        jobId,
                        shuffleWorkerStatusListener,
                        rpcService,
                        testingFatalErrorHandler,
                        ShuffleManagerClientConfiguration.fromConfiguration(configuration),
                        haServices,
                        new HeartbeatServices(1000L, 2000L))) {
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            shuffleManagerClient.start();
            assertThat(
                    clientRegistrationFuture.get(timeout, TimeUnit.MILLISECONDS), equalTo(jobId));

            assertThat(heartbeatFuture.get(timeout, TimeUnit.MILLISECONDS), equalTo(jobId));

            assertEquals(
                    unrelatedShuffleWorkers.get(0),
                    shuffleWorkerStatusListener.pollUnrelatedWorkers(timeout));
            assertEquals(
                    relatedShuffleWorkers.entrySet().stream()
                            .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                            .findFirst()
                            .get(),
                    shuffleWorkerStatusListener.pollRelatedWorkers(timeout));
        }
    }

    @Test
    public void testSynchronizeWorkerStatusWithManager() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        // Registration future
        CompletableFuture<JobID> clientRegistrationFuture = new CompletableFuture<>();
        RegistrationSuccess registrationSuccess =
                new RegistrationSuccess(smGateway.getInstanceID());
        smGateway.setRegisterClientConsumer(
                jobID -> {
                    clientRegistrationFuture.complete(jobID);
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        InstanceID[] workerIds = new InstanceID[3];
        for (int i = 0; i < workerIds.length; ++i) {
            workerIds[i] = new InstanceID("worker" + i);
        }

        Set<InstanceID> initWorkers = new HashSet<>(Arrays.asList(workerIds[0], workerIds[1]));
        smGateway.setHeartbeatFromClientConsumer(
                (jobID, instanceIDS) -> {
                    // Dedicated delay to check if the call is synchronous.
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        // ignored.
                    }

                    assertEquals(initWorkers, instanceIDS);
                    return CompletableFuture.completedFuture(
                            new ManagerToJobHeartbeatPayload(
                                    smGateway.getInstanceID(),
                                    new ChangedWorkerStatus(
                                            Collections.singletonList(workerIds[1]),
                                            Collections.singletonMap(
                                                    workerIds[2], Collections.emptySet()))));
                });
        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        RecordShuffleWorkerStatusListener shuffleWorkerStatusListener =
                new RecordShuffleWorkerStatusListener();
        JobID jobId = RandomIDUtils.randomJobId();
        configuration.setDuration(RpcOptions.RPC_TIMEOUT, Duration.ofSeconds(100));
        try (ShuffleManagerClientImpl shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        jobId,
                        shuffleWorkerStatusListener,
                        rpcService,
                        testingFatalErrorHandler,
                        ShuffleManagerClientConfiguration.fromConfiguration(configuration),
                        haServices,
                        new HeartbeatServices(Long.MAX_VALUE, Long.MAX_VALUE))) {
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));

            // Simulates running in the JM main executor
            jobMainThreadExecutor
                    .submit(
                            () -> {
                                try {
                                    shuffleManagerClient.start();
                                    assertThat(
                                            clientRegistrationFuture.get(
                                                    timeout, TimeUnit.MILLISECONDS),
                                            equalTo(jobId));

                                    shuffleManagerClient.synchronizeWorkerStatus(initWorkers);

                                    assertEquals(
                                            workerIds[1],
                                            shuffleWorkerStatusListener.pollUnrelatedWorkers(100));
                                    assertEquals(
                                            workerIds[2],
                                            shuffleWorkerStatusListener
                                                    .pollRelatedWorkers(100)
                                                    .getLeft());
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            })
                    .get();
        }
    }

    @Test
    public void testHeartbeatTimeoutWithShuffleManager() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        // Registration future
        CompletableFuture<JobID> clientRegistrationFuture = new CompletableFuture<>();
        CountDownLatch registrationAttempts = new CountDownLatch(2);
        RegistrationSuccess registrationSuccess =
                new RegistrationSuccess(smGateway.getInstanceID());
        smGateway.setRegisterClientConsumer(
                jobID -> {
                    clientRegistrationFuture.complete(jobID);
                    registrationAttempts.countDown();
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        // heartbeat future which never terminate to trigger timeout
        smGateway.setHeartbeatFromClientConsumer(
                (jobIds, relatedWorkIds) -> new CompletableFuture<>());

        // Disconnect Future
        CompletableFuture<JobID> clientDisconnectFuture = new CompletableFuture<>();
        smGateway.setUnregisterClientConsumer(
                jobID -> {
                    clientDisconnectFuture.complete(jobID);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        JobID jobId = RandomIDUtils.randomJobId();
        HeartbeatServices heartbeatServices = new HeartbeatServices(1L, 3L);
        try (ShuffleManagerClientImpl shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        jobId,
                        new TestingShuffleWorkerStatusListener(),
                        rpcService,
                        testingFatalErrorHandler,
                        ShuffleManagerClientConfiguration.fromConfiguration(configuration),
                        haServices,
                        heartbeatServices)) {

            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            shuffleManagerClient.start();
            assertThat(
                    clientRegistrationFuture.get(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(jobId));

            assertThat(
                    clientDisconnectFuture.get(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(jobId));

            assertTrue(
                    "The Shuffle Worker should try to reconnect to the RM",
                    registrationAttempts.await(timeout, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testUnMonitorShuffleManagerOnLeadershipRevoked() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        JobID jobId = RandomIDUtils.randomJobId();
        RecordingHeartbeatServices heartbeatServices = new RecordingHeartbeatServices(1L, 100000L);
        try (ShuffleManagerClientImpl shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        jobId,
                        new TestingShuffleWorkerStatusListener(),
                        rpcService,
                        testingFatalErrorHandler,
                        ShuffleManagerClientConfiguration.fromConfiguration(configuration),
                        haServices,
                        heartbeatServices)) {
            BlockingQueue<InstanceID> monitoredTargets = heartbeatServices.getMonitoredTargets();
            BlockingQueue<InstanceID> unmonitoredTargets =
                    heartbeatServices.getUnmonitoredTargets();

            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            shuffleManagerClient.start();

            assertThat(
                    monitoredTargets.poll(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(smGateway.getInstanceID()));

            shuffleManagerLeaderRetrieveService.notifyListener(LeaderInformation.empty());
            assertThat(
                    unmonitoredTargets.poll(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(smGateway.getInstanceID()));
        }
    }

    @Test
    public void testRequestAndReleaseShuffleResource() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        // Registration future
        CompletableFuture<JobID> clientRegistrationFuture = new CompletableFuture<>();
        CountDownLatch registrationAttempts = new CountDownLatch(2);
        RegistrationSuccess registrationSuccess =
                new RegistrationSuccess(smGateway.getInstanceID());
        smGateway.setRegisterClientConsumer(
                jobID -> {
                    clientRegistrationFuture.complete(jobID);
                    registrationAttempts.countDown();
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        // Resource request
        CompletableFuture<Tuple4<JobID, DataSetID, MapPartitionID, Integer>> resourceRequestFuture =
                new CompletableFuture<>();
        ShuffleResource shuffleResource =
                new DefaultShuffleResource(
                        new ShuffleWorkerDescriptor[] {
                            new ShuffleWorkerDescriptor(new InstanceID("worker1"), "worker1", 20480)
                        },
                        DataPartition.DataPartitionType.MAP_PARTITION,
                        DiskType.ANY_TYPE);
        smGateway.setAllocateShuffleResourceConsumer(
                (jobID, dataSetID, mapPartitionID, numberOfSubpartitions) -> {
                    resourceRequestFuture.complete(
                            new Tuple4<>(jobID, dataSetID, mapPartitionID, numberOfSubpartitions));
                    return CompletableFuture.completedFuture(shuffleResource);
                });

        // Resource release
        CompletableFuture<Triple<JobID, DataSetID, MapPartitionID>> resourceReleaseFuture =
                new CompletableFuture<>();
        smGateway.setReleaseShuffleResourceConsumer(
                (jobID, dataSetID, mapPartitionID) -> {
                    resourceReleaseFuture.complete(Triple.of(jobID, dataSetID, mapPartitionID));
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        JobID jobId = RandomIDUtils.randomJobId();
        DataSetID dataSetId = RandomIDUtils.randomDataSetId();
        MapPartitionID dataPartitionId = RandomIDUtils.randomMapPartitionId();
        int numberOfSubpartitions = 10;

        try (ShuffleManagerClientImpl shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        jobId,
                        new TestingShuffleWorkerStatusListener(),
                        rpcService,
                        testingFatalErrorHandler,
                        ShuffleManagerClientConfiguration.fromConfiguration(configuration),
                        haServices,
                        new HeartbeatServices(1000L, 3000L))) {

            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            shuffleManagerClient.start();

            CompletableFuture<ShuffleResource> result1 =
                    shuffleManagerClient.requestShuffleResource(
                            dataSetId,
                            dataPartitionId,
                            numberOfSubpartitions,
                            0,
                            partitionFactoryName,
                            null);
            result1.join();

            assertThat(
                    clientRegistrationFuture.get(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(jobId));
            assertThat(
                    resourceRequestFuture.get(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(
                            new Tuple4<>(
                                    jobId, dataSetId, dataPartitionId, numberOfSubpartitions)));
            assertTrue(result1.isDone());
            assertEquals(shuffleResource, result1.get());

            shuffleManagerClient.releaseShuffleResource(dataSetId, dataPartitionId);
            assertThat(
                    resourceReleaseFuture.get(timeout, TimeUnit.MILLISECONDS),
                    Matchers.equalTo(Triple.of(jobId, dataSetId, dataPartitionId)));
        }
    }

    @Test
    public void testNotifyNewShuffleManagerLeaderAfterClosed() {
        ShuffleManagerClientImpl shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        RandomIDUtils.randomJobId(),
                        new RecordShuffleWorkerStatusListener(),
                        rpcService,
                        testingFatalErrorHandler,
                        ShuffleManagerClientConfiguration.fromConfiguration(configuration),
                        haServices,
                        new HeartbeatServices(Long.MAX_VALUE, Long.MAX_VALUE));
        shuffleManagerClient.close();
        // this should not throw any exception
        shuffleManagerClient.notifyLeaderAddress(LeaderInformation.empty());
    }

    private static class RecordShuffleWorkerStatusListener implements ShuffleWorkerStatusListener {

        private final BlockingQueue<InstanceID> unrelatedWorkers = new LinkedBlockingQueue<>();
        private final BlockingQueue<Pair<InstanceID, Collection<DataPartitionCoordinate>>>
                relatedWorkers = new LinkedBlockingQueue<>();

        @Override
        public void notifyIrrelevantWorker(InstanceID workerID) {
            unrelatedWorkers.add(workerID);
        }

        @Override
        public void notifyRelevantWorker(
                InstanceID workerID, Set<DataPartitionCoordinate> dataPartitions) {
            relatedWorkers.add(Pair.of(workerID, dataPartitions));
        }

        public InstanceID pollUnrelatedWorkers(long timeout) throws InterruptedException {
            return unrelatedWorkers.poll(timeout, TimeUnit.MILLISECONDS);
        }

        public Pair<InstanceID, Collection<DataPartitionCoordinate>> pollRelatedWorkers(
                long timeout) throws InterruptedException {
            return relatedWorkers.poll(timeout, TimeUnit.MILLISECONDS);
        }
    }
}
