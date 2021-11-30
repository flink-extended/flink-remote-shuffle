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

package com.alibaba.flink.shuffle.coordinator.worker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServicesUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.TestingHaServices;
import com.alibaba.flink.shuffle.coordinator.leaderretrieval.SettableLeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerRegistrationSuccess;
import com.alibaba.flink.shuffle.coordinator.manager.WorkerToManagerHeartbeatPayload;
import com.alibaba.flink.shuffle.coordinator.utils.EmptyMetaStore;
import com.alibaba.flink.shuffle.coordinator.utils.EmptyPartitionedDataStore;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.coordinator.utils.RecordingHeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.utils.TestingFatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.utils.TestingShuffleManagerGateway;
import com.alibaba.flink.shuffle.coordinator.worker.metastore.LocalShuffleMetaStore;
import com.alibaba.flink.shuffle.coordinator.worker.metastore.LocalShuffleMetaStoreTest;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;
import com.alibaba.flink.shuffle.rpc.test.TestingRpcService;
import com.alibaba.flink.shuffle.transfer.NettyConfig;
import com.alibaba.flink.shuffle.transfer.NettyServer;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests the behavior of the {@link ShuffleWorker}. */
public class ShuffleWorkerTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private static final long timeout = 10000L;

    private TestingRpcService rpcService;

    private Configuration configuration;

    private SettableLeaderRetrievalService shuffleManagerLeaderRetrieveService;

    private TestingHaServices haServices;

    private ShuffleWorkerLocation shuffleWorkerLocation;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    private PartitionedDataStore dataStore;

    private NettyServer nettyServer;

    @Before
    public void setup() throws IOException {
        rpcService = new TestingRpcService();

        configuration = new Configuration();

        shuffleManagerLeaderRetrieveService = new SettableLeaderRetrievalService();
        haServices = new TestingHaServices();
        haServices.setShuffleManagerLeaderRetrieveService(shuffleManagerLeaderRetrieveService);

        shuffleWorkerLocation = new LocalShuffleWorkerLocation();

        testingFatalErrorHandler = new TestingFatalErrorHandler();

        dataStore = new EmptyPartitionedDataStore();

        // choose random worker port
        Random random = new Random(System.currentTimeMillis());
        int nextPort = random.nextInt(30000) + 20000;
        configuration.setInteger(TransferOptions.SERVER_DATA_PORT, nextPort);
        NettyConfig nettyConfig = new NettyConfig(configuration);
        nettyServer = new NettyServer(dataStore, nettyConfig);

        // By default, we do not start netty server.
    }

    @After
    public void teardown() throws Exception {
        if (rpcService != null) {
            rpcService.stopService().get(timeout, TimeUnit.MILLISECONDS);
            rpcService = null;
        }

        if (nettyServer != null) {
            nettyServer.shutdown();
        }
    }

    @Test
    public void testRegisterAndReportDataPartitionStatus() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        CompletableFuture<InstanceID> registrationFuture = new CompletableFuture<>();
        ShuffleWorkerRegistrationSuccess registrationSuccess =
                new ShuffleWorkerRegistrationSuccess(
                        new RegistrationID(), smGateway.getInstanceID());
        smGateway.setRegisterShuffleWorkerConsumer(
                registration -> {
                    registrationFuture.complete(registration.getWorkerID());
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        List<DataPartitionStatus> reportedStatuses =
                Arrays.asList(
                        new DataPartitionStatus(
                                RandomIDUtils.randomJobId(),
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId()),
                                false),
                        new DataPartitionStatus(
                                RandomIDUtils.randomJobId(),
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId()),
                                true));
        CompletableFuture<List<DataPartitionStatus>> reportedDataPartitionsFuture =
                new CompletableFuture<>();
        smGateway.setReportShuffleDataStatusConsumer(
                (resourceID, instanceID, dataPartitionStatuses) -> {
                    reportedDataPartitionsFuture.complete(dataPartitionStatuses);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        try (ShuffleWorker shuffleWorker =
                new ShuffleWorker(
                        rpcService,
                        ShuffleWorkerConfiguration.fromConfiguration(configuration),
                        haServices,
                        HeartbeatServicesUtils.createManagerWorkerHeartbeatServices(configuration),
                        testingFatalErrorHandler,
                        shuffleWorkerLocation,
                        new EmptyMetaStore() {
                            @Override
                            public List<DataPartitionStatus> listDataPartitions() throws Exception {
                                return reportedStatuses;
                            }
                        },
                        dataStore,
                        nettyServer)) {
            shuffleWorker.start();
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            assertThat(
                    registrationFuture.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(shuffleWorkerLocation.getWorkerID()));

            assertThat(
                    reportedDataPartitionsFuture.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(reportedStatuses));
        }
    }

    @Test
    public void testHeartbeatTimeoutWithShuffleManager() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        CompletableFuture<InstanceID> registrationFuture = new CompletableFuture<>();
        CountDownLatch registrationAttempts = new CountDownLatch(2);
        ShuffleWorkerRegistrationSuccess registrationSuccess =
                new ShuffleWorkerRegistrationSuccess(
                        new RegistrationID(), smGateway.getInstanceID());
        smGateway.setRegisterShuffleWorkerConsumer(
                registration -> {
                    registrationFuture.complete(registration.getWorkerID());
                    registrationAttempts.countDown();
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        CompletableFuture<InstanceID> shuffleWorkerDisconnectFuture = new CompletableFuture<>();
        smGateway.setDisconnectShuffleWorkerConsumer(
                (resourceID, e) -> {
                    shuffleWorkerDisconnectFuture.complete(resourceID);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        HeartbeatServices heartbeatServices = new HeartbeatServices(1L, 3L);
        try (ShuffleWorker shuffleWorker =
                new ShuffleWorker(
                        rpcService,
                        ShuffleWorkerConfiguration.fromConfiguration(configuration),
                        haServices,
                        heartbeatServices,
                        testingFatalErrorHandler,
                        shuffleWorkerLocation,
                        new EmptyMetaStore(),
                        dataStore,
                        nettyServer)) {
            shuffleWorker.start();
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            assertThat(
                    registrationFuture.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(shuffleWorkerLocation.getWorkerID()));

            assertThat(
                    shuffleWorkerDisconnectFuture.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(shuffleWorkerLocation.getWorkerID()));

            assertTrue(
                    "The Shuffle Worker should try to reconnect to the RM",
                    registrationAttempts.await(timeout, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testHeartbeatReporting() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        // Registration future
        CompletableFuture<InstanceID> registrationFuture = new CompletableFuture<>();
        CountDownLatch registrationAttempts = new CountDownLatch(2);
        ShuffleWorkerRegistrationSuccess registrationSuccess =
                new ShuffleWorkerRegistrationSuccess(
                        new RegistrationID(), smGateway.getInstanceID());
        smGateway.setRegisterShuffleWorkerConsumer(
                registration -> {
                    registrationFuture.complete(registration.getWorkerID());
                    registrationAttempts.countDown();
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        // Initial report future
        CompletableFuture<List<DataPartitionStatus>> reportedDataPartitionsFuture =
                new CompletableFuture<>();
        smGateway.setReportShuffleDataStatusConsumer(
                (resourceID, instanceID, dataPartitionStatuses) -> {
                    reportedDataPartitionsFuture.complete(dataPartitionStatuses);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        // Heartbeat future
        CompletableFuture<WorkerToManagerHeartbeatPayload> heartbeatPayloadCompletableFuture =
                new CompletableFuture<>();
        smGateway.setHeartbeatFromShuffleWorkerConsumer(
                (resourceID, shuffleWorkerToManagerHeartbeatPayload) ->
                        heartbeatPayloadCompletableFuture.complete(
                                shuffleWorkerToManagerHeartbeatPayload));

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        List<DataPartitionStatus> reportedStatuses1 =
                Arrays.asList(
                        new DataPartitionStatus(
                                RandomIDUtils.randomJobId(),
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId()),
                                false),
                        new DataPartitionStatus(
                                RandomIDUtils.randomJobId(),
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId()),
                                true));
        List<DataPartitionStatus> reportedStatuses2 =
                Arrays.asList(
                        new DataPartitionStatus(
                                RandomIDUtils.randomJobId(),
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId()),
                                false),
                        new DataPartitionStatus(
                                RandomIDUtils.randomJobId(),
                                new DataPartitionCoordinate(
                                        RandomIDUtils.randomDataSetId(),
                                        RandomIDUtils.randomMapPartitionId()),
                                true));
        Queue<List<DataPartitionStatus>> reportedStatusesQueue =
                new ArrayDeque<>(Arrays.asList(reportedStatuses1, reportedStatuses2));

        HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 2000L);
        try (ShuffleWorker shuffleWorker =
                new ShuffleWorker(
                        rpcService,
                        ShuffleWorkerConfiguration.fromConfiguration(configuration),
                        haServices,
                        heartbeatServices,
                        testingFatalErrorHandler,
                        shuffleWorkerLocation,
                        new EmptyMetaStore() {
                            @Override
                            public List<DataPartitionStatus> listDataPartitions() throws Exception {
                                return reportedStatusesQueue.poll();
                            }
                        },
                        dataStore,
                        nettyServer)) {
            shuffleWorker.start();
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            assertThat(
                    registrationFuture.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(shuffleWorkerLocation.getWorkerID()));

            assertThat(
                    reportedDataPartitionsFuture.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(reportedStatuses1));

            ShuffleWorkerGateway shuffleWorkerGateway =
                    shuffleWorker.getSelfGateway(ShuffleWorkerGateway.class);
            shuffleWorkerGateway.heartbeatFromManager(smGateway.getInstanceID());
            assertThat(
                    heartbeatPayloadCompletableFuture
                            .get(timeout, TimeUnit.MILLISECONDS)
                            .getDataPartitionStatuses(),
                    equalTo(reportedStatuses2));
        }
    }

    @Test
    public void testUnMonitorShuffleManagerOnLeadershipRevoked() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        RecordingHeartbeatServices heartbeatServices = new RecordingHeartbeatServices(1L, 100000L);
        try (ShuffleWorker shuffleWorker =
                new ShuffleWorker(
                        rpcService,
                        ShuffleWorkerConfiguration.fromConfiguration(configuration),
                        haServices,
                        heartbeatServices,
                        testingFatalErrorHandler,
                        shuffleWorkerLocation,
                        new EmptyMetaStore(),
                        dataStore,
                        nettyServer)) {
            shuffleWorker.start();

            BlockingQueue<InstanceID> monitoredTargets = heartbeatServices.getMonitoredTargets();
            BlockingQueue<InstanceID> unmonitoredTargets =
                    heartbeatServices.getUnmonitoredTargets();

            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            assertThat(
                    monitoredTargets.poll(timeout, TimeUnit.MILLISECONDS),
                    equalTo(smGateway.getInstanceID()));

            shuffleManagerLeaderRetrieveService.notifyListener(LeaderInformation.empty());
            assertThat(
                    unmonitoredTargets.poll(timeout, TimeUnit.MILLISECONDS),
                    equalTo(smGateway.getInstanceID()));
        }
    }

    @Test
    public void testReconnectionAttemptIfExplicitlyDisconnected() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        BlockingQueue<InstanceID> registrationQueue = new ArrayBlockingQueue<>(1);
        ShuffleWorkerRegistrationSuccess registrationSuccess =
                new ShuffleWorkerRegistrationSuccess(
                        new RegistrationID(), smGateway.getInstanceID());
        smGateway.setRegisterShuffleWorkerConsumer(
                registration -> {
                    registrationQueue.add(registration.getWorkerID());
                    return CompletableFuture.completedFuture(registrationSuccess);
                });

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        try (ShuffleWorker shuffleWorker =
                new ShuffleWorker(
                        rpcService,
                        ShuffleWorkerConfiguration.fromConfiguration(configuration),
                        haServices,
                        HeartbeatServicesUtils.createManagerWorkerHeartbeatServices(configuration),
                        testingFatalErrorHandler,
                        shuffleWorkerLocation,
                        new EmptyMetaStore(),
                        dataStore,
                        nettyServer)) {
            shuffleWorker.start();
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            InstanceID firstRegistrationAttempt = registrationQueue.take();
            assertEquals(shuffleWorkerLocation.getWorkerID(), firstRegistrationAttempt);
            assertEquals(0, registrationQueue.size());

            ShuffleWorkerGateway shuffleWorkerGateway =
                    shuffleWorker.getSelfGateway(ShuffleWorkerGateway.class);
            shuffleWorkerGateway.disconnectManager(new Exception("Test exception"));
            InstanceID secondAttempt = registrationQueue.take();
            assertEquals(shuffleWorkerLocation.getWorkerID(), secondAttempt);
        }
    }

    @Test
    public void testNotifyManagerOnPartitionRemoved() throws Exception {
        TestingShuffleManagerGateway smGateway = new TestingShuffleManagerGateway();

        // Initial report future
        CompletableFuture<List<DataPartitionStatus>> reportedDataPartitionsFuture =
                new CompletableFuture<>();
        smGateway.setReportShuffleDataStatusConsumer(
                (resourceID, instanceID, dataPartitionStatuses) -> {
                    reportedDataPartitionsFuture.complete(dataPartitionStatuses);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        // Release future
        CompletableFuture<Triple<JobID, DataSetID, DataPartitionID>> releasedPartition =
                new CompletableFuture<>();
        smGateway.setWorkerReportDataPartitionReleasedConsumer(
                (tmIds, dataPartitionIds) -> {
                    releasedPartition.complete(dataPartitionIds);
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        // Create the test metastore
        File baseDir = TEMP_FOLDER.newFolder();
        String basePath = baseDir.getAbsolutePath() + "/";
        LocalShuffleMetaStore metaStore =
                new LocalShuffleMetaStore(Collections.singleton(basePath));
        DataPartitionMeta meta = LocalShuffleMetaStoreTest.randomMeta(basePath);
        metaStore.onPartitionCreated(meta);

        try (ShuffleWorker shuffleWorker =
                new ShuffleWorker(
                        rpcService,
                        ShuffleWorkerConfiguration.fromConfiguration(configuration),
                        haServices,
                        HeartbeatServicesUtils.createManagerWorkerHeartbeatServices(configuration),
                        testingFatalErrorHandler,
                        shuffleWorkerLocation,
                        metaStore,
                        dataStore,
                        nettyServer)) {
            shuffleWorker.start();
            shuffleManagerLeaderRetrieveService.notifyListener(
                    new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
            assertThat(
                    reportedDataPartitionsFuture.get(timeout, TimeUnit.MILLISECONDS).size(),
                    equalTo(1));

            metaStore.onPartitionRemoved(meta);
            assertThat(
                    releasedPartition.get(timeout, TimeUnit.MILLISECONDS),
                    equalTo(
                            Triple.of(
                                    meta.getJobID(),
                                    meta.getDataSetID(),
                                    meta.getDataPartitionID())));
        }
    }
}
