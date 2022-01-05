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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManagerImpl;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.NoOpHeartbeatManager;
import com.alibaba.flink.shuffle.coordinator.highavailability.TestingHighAvailabilityServices;
import com.alibaba.flink.shuffle.coordinator.leaderelection.TestingLeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.AssignmentTrackerImpl;
import com.alibaba.flink.shuffle.coordinator.registration.RegistrationResponse;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.coordinator.utils.TestingFatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.utils.TestingUtils;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.utils.OneShotLatch;
import com.alibaba.flink.shuffle.core.utils.TestLogger;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;
import com.alibaba.flink.shuffle.rpc.test.TestingRpcService;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test ShuffleManager. */
public class ShuffleManagerTest extends TestLogger {

    private static final String partitionFactoryName =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory";

    private static final long TIMEOUT = 10000L;

    private static final long HEARTBEAT_TIMEOUT = 5000;

    private static TestingRpcService rpcService;

    private TestingLeaderElectionService leaderElectionService;

    private ShuffleManager shuffleManager;

    private TestingFatalErrorHandler testingFatalErrorHandler;

    private ShuffleManagerGateway shuffleManagerGateway;

    private ShuffleManagerGateway wronglyFencedGateway;

    private AssignmentTrackerImpl testAssignmentTracker;

    @BeforeClass
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @AfterClass
    public static void teardownClass() throws Exception {
        if (rpcService != null) {
            rpcService.stopService().get(TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    @Before
    public void setup() throws Exception {
        shuffleManagerGateway = initializeServicesAndShuffleManagerGateway();
        createShuffleWorkerGateway();
    }

    @After
    public void teardown() throws Exception {
        if (shuffleManager != null) {
            shuffleManager.closeAsync().get(TIMEOUT, TimeUnit.MILLISECONDS);
        }

        if (testingFatalErrorHandler != null && testingFatalErrorHandler.hasExceptionOccurred()) {
            testingFatalErrorHandler.rethrowError();
        }

        rpcService.clearGateways();
    }

    @Test
    public void testRegisterShuffleWorker()
            throws InterruptedException, ExecutionException, TimeoutException {

        assertEquals(0, testAssignmentTracker.getWorkers().size());
        CompletableFuture<RegistrationResponse> successfulFuture =
                shuffleManagerGateway.registerWorker(
                        TestShuffleWorkerGateway.createShuffleWorkerRegistration());
        RegistrationResponse response = successfulFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(response instanceof ShuffleWorkerRegistrationSuccess);
        assertEquals(
                Collections.singleton(
                        ((ShuffleWorkerRegistrationSuccess) response).getRegistrationID()),
                testAssignmentTracker.getWorkers().keySet());

        // test response successful with instanceID not equal to previous when receive duplicate
        // registration from shuffleWorker
        CompletableFuture<RegistrationResponse> duplicateFuture =
                shuffleManagerGateway.registerWorker(
                        TestShuffleWorkerGateway.createShuffleWorkerRegistration());

        RegistrationResponse duplicateResponse = duplicateFuture.get();
        assertTrue(duplicateResponse instanceof ShuffleWorkerRegistrationSuccess);
        assertNotEquals(
                ((ShuffleWorkerRegistrationSuccess) response).getRegistrationID(),
                ((ShuffleWorkerRegistrationSuccess) duplicateResponse).getRegistrationID());

        assertEquals(
                Collections.singleton(
                        ((ShuffleWorkerRegistrationSuccess) duplicateResponse).getRegistrationID()),
                testAssignmentTracker.getWorkers().keySet());
    }

    @Test(timeout = 20000)
    public void testRevokeAndGrantLeadership() throws Exception {
        assertNotEquals(
                NoOpHeartbeatManager.class, shuffleManager.getWorkerHeartbeatManager().getClass());
        assertNotEquals(
                NoOpHeartbeatManager.class, shuffleManager.getJobHeartbeatManager().getClass());

        ShuffleWorkerRegistration registration =
                TestShuffleWorkerGateway.createShuffleWorkerRegistration();
        CompletableFuture<RegistrationResponse> successfulFuture =
                shuffleManagerGateway.registerWorker(registration);
        RegistrationResponse response = successfulFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(response instanceof ShuffleWorkerRegistrationSuccess);

        // The initial state
        assertEquals(
                Collections.singleton(registration.getWorkerID()),
                shuffleManager.getShuffleWorkers().keySet());
        assertEquals(
                Collections.singleton(
                        ((ShuffleWorkerRegistrationSuccess) successfulFuture.get())
                                .getRegistrationID()),
                testAssignmentTracker.getWorkers().keySet());
        assertTrue(
                ((HeartbeatManagerImpl<?, ?>) shuffleManager.getWorkerHeartbeatManager())
                        .getHeartbeatTargets()
                        .containsKey(registration.getWorkerID()));

        leaderElectionService.notLeader();
        grantLeadership(leaderElectionService);

        // Right after re-grant leadership
        assertEquals(
                Collections.singleton(registration.getWorkerID()),
                shuffleManager.getShuffleWorkers().keySet());
        assertEquals(
                Collections.singleton(
                        ((ShuffleWorkerRegistrationSuccess) successfulFuture.get())
                                .getRegistrationID()),
                testAssignmentTracker.getWorkers().keySet());
        assertTrue(
                ((HeartbeatManagerImpl<?, ?>) shuffleManager.getWorkerHeartbeatManager())
                        .getHeartbeatTargets()
                        .containsKey(registration.getWorkerID()));

        // The shuffle worker would then come to disconnect itself.
        shuffleManager.disconnectWorker(registration.getWorkerID(), new RuntimeException("Test"));

        // After the worker disconnect
        assertEquals(0, shuffleManager.getShuffleWorkers().size());
        assertEquals(
                0,
                ((AssignmentTrackerImpl) shuffleManager.getAssignmentTracker())
                        .getWorkers()
                        .size());
        assertFalse(
                ((HeartbeatManagerImpl<?, ?>) shuffleManager.getWorkerHeartbeatManager())
                        .getHeartbeatTargets()
                        .containsKey(registration.getWorkerID()));
    }

    /**
     * Tests delayed registration of shuffle worker where the delay is introduced during connection
     * from shuffle manager to the registering shuffle worker.
     */
    @Test
    public void testDelayedRegisterShuffleWorker() throws Exception {
        try {
            final OneShotLatch startConnection = new OneShotLatch();
            final OneShotLatch finishConnection = new OneShotLatch();

            // first registration is with blocking connection
            rpcService.setRpcGatewayFutureFunction(
                    rpcGateway ->
                            CompletableFuture.supplyAsync(
                                    () -> {
                                        startConnection.trigger();
                                        try {
                                            finishConnection.await();
                                        } catch (InterruptedException ignored) {
                                        }
                                        return rpcGateway;
                                    },
                                    TestingUtils.defaultScheduledExecutor()));

            CompletableFuture<RegistrationResponse> firstFuture =
                    shuffleManagerGateway.registerWorker(
                            TestShuffleWorkerGateway.createShuffleWorkerRegistration());
            try {
                firstFuture.get();
                fail(
                        "Should have failed because connection to shuffle worker is delayed beyond timeout");
            } catch (Exception e) {
                final Throwable cause = ExceptionUtils.stripException(e, ExecutionException.class);
                assertTrue(cause instanceof TimeoutException);
                assertTrue(cause.getMessage().contains("ShuffleManagerGateway.registerWorker"));
            }

            startConnection.await();

            // second registration after timeout is with no delay, expecting it to be succeeded
            rpcService.resetRpcGatewayFutureFunction();
            CompletableFuture<RegistrationResponse> secondFuture =
                    shuffleManagerGateway.registerWorker(
                            TestShuffleWorkerGateway.createShuffleWorkerRegistration());
            RegistrationResponse response = secondFuture.get();
            assertTrue(response instanceof ShuffleWorkerRegistrationSuccess);

            // on success, send data partition report for shuffle manager

            shuffleManagerGateway
                    .reportDataPartitionStatus(
                            TestShuffleWorkerGateway.getShuffleWorkerID(),
                            ((ShuffleWorkerRegistrationSuccess) response).getRegistrationID(),
                            Collections.singletonList(createDataPartitionStatus()))
                    .get();

            // let the remaining part of the first registration proceed
            finishConnection.trigger();
            Thread.sleep(1L);

            // verify that the latest registration is valid not being unregistered by the delayed
            // one
            assertEquals(1, testAssignmentTracker.getWorkers().size());
        } finally {
            rpcService.resetRpcGatewayFutureFunction();
        }
    }

    /** Tests that a shuffle worker can disconnect from the shuffle manager. */
    @Test
    public void testDisconnectShuffleWorker() throws Exception {
        CompletableFuture<RegistrationResponse> successfulFuture =
                shuffleManagerGateway.registerWorker(
                        TestShuffleWorkerGateway.createShuffleWorkerRegistration());

        RegistrationResponse response = successfulFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(response instanceof ShuffleWorkerRegistrationSuccess);
        assertEquals(1, testAssignmentTracker.getWorkers().size());

        shuffleManagerGateway
                .disconnectWorker(
                        TestShuffleWorkerGateway.getShuffleWorkerID(),
                        new RuntimeException("testDisconnectShuffleWorker"))
                .get();

        assertEquals(0, testAssignmentTracker.getWorkers().size());
    }

    /** Test receive registration with unmatched leadershipId from shuffle worker. */
    @Test
    public void testRegisterShuffleWorkerWithUnmatchedLeaderSessionId() throws Exception {
        // test throw exception when receive a registration from a shuffle worker which takes
        // unmatched
        // leaderSessionId
        CompletableFuture<RegistrationResponse> unMatchedLeaderFuture =
                wronglyFencedGateway.registerWorker(
                        TestShuffleWorkerGateway.createShuffleWorkerRegistration());

        try {
            unMatchedLeaderFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
            fail("Should have failed because we are using a wrongly fenced ShuffleManagerGateway.");
        } catch (ExecutionException e) {
            assertTrue(
                    ExceptionUtils.stripException(e, ExecutionException.class)
                            .getMessage()
                            .contains("Fencing token mismatch"));
        }
    }

    /** Test receive registration with invalid address from shuffle worker. */
    @Test
    public void testRegisterShuffleWorkerFromInvalidAddress() throws Exception {
        // test throw exception when receive a registration from shuffle worker which takes invalid
        // address
        String invalidAddress = "/shuffleworker2";

        CompletableFuture<RegistrationResponse> invalidAddressFuture =
                shuffleManagerGateway.registerWorker(
                        TestShuffleWorkerGateway.createShuffleWorkerRegistration(invalidAddress));
        assertTrue(
                invalidAddressFuture.get(TIMEOUT, TimeUnit.MILLISECONDS)
                        instanceof RegistrationResponse.Decline);
    }

    @Test(timeout = 30000L)
    public void testShuffleClientRegisterAndUnregister()
            throws ExecutionException, InterruptedException {
        assertEquals(0, testAssignmentTracker.getJobs().size());
        final JobID jobID = RandomIDUtils.randomJobId();
        final InstanceID instanceId = new InstanceID();
        shuffleManagerGateway.registerClient(jobID, instanceId).get();
        assertEquals(1, testAssignmentTracker.getJobs().size());

        assertNotNull(testAssignmentTracker.getJobs().get(jobID));

        shuffleManagerGateway.unregisterClient(jobID, instanceId).get();
        assertEquals(Collections.singleton(jobID), testAssignmentTracker.getJobs().keySet());

        // Here we remove the instance id mapping.
        assertEquals(0, shuffleManager.getRegisteredClients().size());

        // Wait till the client finally get unregistered via timeout
        while (shuffleManager.getAssignmentTracker().isJobRegistered(jobID)) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testShuffleClientRegisterAndUnregisterAndReconnect()
            throws ExecutionException, InterruptedException {
        assertEquals(0, testAssignmentTracker.getJobs().size());

        // A hacky way to register a worker
        shuffleManager
                .getAssignmentTracker()
                .registerWorker(
                        new InstanceID("worker1"),
                        new RegistrationID(),
                        new TestShuffleWorkerGateway(),
                        "localhost",
                        10240);

        final JobID jobID = RandomIDUtils.randomJobId();
        final InstanceID instanceId = new InstanceID();
        shuffleManagerGateway.registerClient(jobID, instanceId).get();
        assertEquals(1, testAssignmentTracker.getJobs().size());

        assertNotNull(testAssignmentTracker.getJobs().get(jobID));
        DataSetID dataSetID = RandomIDUtils.randomDataSetId();
        MapPartitionID dataPartitionID = RandomIDUtils.randomMapPartitionId();
        shuffleManagerGateway
                .requestShuffleResource(
                        jobID, instanceId, dataSetID, dataPartitionID, 1, partitionFactoryName)
                .get();

        shuffleManagerGateway.unregisterClient(jobID, instanceId).get();
        assertEquals(Collections.singleton(jobID), testAssignmentTracker.getJobs().keySet());

        InstanceID instanceID2 = new InstanceID();
        shuffleManager.registerClient(jobID, instanceID2).get();
        ManagerToJobHeartbeatPayload payload =
                shuffleManager
                        .heartbeatFromClient(jobID, instanceID2, Collections.emptySet())
                        .get();
        assertEquals(
                Collections.singleton(new InstanceID("worker1")),
                payload.getJobChangedWorkerStatus().getRelevantWorkers().keySet());
    }

    @Test
    public void testAllocateShuffleResource()
            throws InterruptedException, ExecutionException, TimeoutException {
        // register shuffle worker
        CompletableFuture<RegistrationResponse> successfulFuture =
                shuffleManagerGateway.registerWorker(
                        TestShuffleWorkerGateway.createShuffleWorkerRegistration());
        RegistrationResponse response = successfulFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
        assertTrue(response instanceof ShuffleWorkerRegistrationSuccess);

        // register shuffle client
        final JobID jobID = RandomIDUtils.randomJobId();
        final InstanceID instanceID = new InstanceID();
        final DataSetID dataSetID = RandomIDUtils.randomDataSetId();
        final MapPartitionID mapPartitionID = RandomIDUtils.randomMapPartitionId();
        shuffleManagerGateway.registerClient(jobID, instanceID).get();

        // allocate shuffle resource
        ShuffleResource shuffleResource =
                shuffleManagerGateway
                        .requestShuffleResource(
                                jobID,
                                instanceID,
                                dataSetID,
                                mapPartitionID,
                                128,
                                partitionFactoryName)
                        .get();
        assertTrue(shuffleResource instanceof DefaultShuffleResource);
        DefaultShuffleResource defaultShuffleResource = (DefaultShuffleResource) shuffleResource;
        assertEquals(
                TestShuffleWorkerGateway.getShuffleWorkerID(),
                defaultShuffleResource.getMapPartitionLocation().getWorkerId());
        assertNotNull(testAssignmentTracker.getJobs().get(jobID));
        assertEquals(1, testAssignmentTracker.getJobs().get(jobID).getDataPartitions().size());

        // release allocated resources

        shuffleManagerGateway
                .releaseShuffleResource(jobID, instanceID, dataSetID, mapPartitionID)
                .get();
        assertEquals(0, testAssignmentTracker.getJobs().get(jobID).getDataPartitions().size());
    }

    @Test
    public void testInconsistentInstanceId() throws ExecutionException, InterruptedException {
        // register shuffle client
        final JobID jobID = RandomIDUtils.randomJobId();
        final InstanceID instanceID = new InstanceID();
        shuffleManagerGateway.registerClient(jobID, instanceID).get();

        {
            CompletableFuture<?> result =
                    shuffleManagerGateway.heartbeatFromClient(
                            jobID, new InstanceID(), Collections.emptySet());
            assertFailedWithInconsistentInstanceId(result);
        }

        {
            CompletableFuture<?> result =
                    shuffleManagerGateway.requestShuffleResource(
                            jobID,
                            new InstanceID(),
                            RandomIDUtils.randomDataSetId(),
                            RandomIDUtils.randomMapPartitionId(),
                            2,
                            partitionFactoryName);
            assertFailedWithInconsistentInstanceId(result);
        }

        {
            CompletableFuture<?> result =
                    shuffleManagerGateway.releaseShuffleResource(
                            jobID,
                            new InstanceID(),
                            RandomIDUtils.randomDataSetId(),
                            RandomIDUtils.randomMapPartitionId());
            assertFailedWithInconsistentInstanceId(result);
        }

        {
            CompletableFuture<?> result =
                    shuffleManagerGateway.unregisterClient(jobID, new InstanceID());
            assertFailedWithInconsistentInstanceId(result);
        }
    }

    @Test
    public void testShuffleWorkerReportDataPartitionWithoutPendingJob() throws Exception {
        ShuffleWorkerRegistration worker =
                TestShuffleWorkerGateway.createShuffleWorkerRegistration();
        ShuffleWorkerRegistrationSuccess response =
                (ShuffleWorkerRegistrationSuccess) shuffleManager.registerWorker(worker).get();

        DataPartitionStatus dataPartitionStatus =
                new DataPartitionStatus(
                        RandomIDUtils.randomJobId(),
                        new DataPartitionCoordinate(
                                RandomIDUtils.randomDataSetId(),
                                RandomIDUtils.randomMapPartitionId()));
        DataPartitionStatus releasingDataPartitionStatus =
                new DataPartitionStatus(
                        RandomIDUtils.randomJobId(),
                        new DataPartitionCoordinate(
                                RandomIDUtils.randomDataSetId(),
                                RandomIDUtils.randomMapPartitionId()));

        shuffleManager
                .reportDataPartitionStatus(
                        worker.getWorkerID(),
                        response.getRegistrationID(),
                        Arrays.asList(dataPartitionStatus, releasingDataPartitionStatus))
                .get();

        assertTrue(
                shuffleManager
                        .getAssignmentTracker()
                        .isJobRegistered(dataPartitionStatus.getJobId()));
        assertTrue(
                shuffleManager
                        .getAssignmentTracker()
                        .isJobRegistered(releasingDataPartitionStatus.getJobId()));
    }

    @Test
    public void testShuffleWorkerHeartbeatWithoutPendingJobs() throws Exception {
        ShuffleWorkerRegistration worker =
                TestShuffleWorkerGateway.createShuffleWorkerRegistration();
        ShuffleWorkerRegistrationSuccess response =
                (ShuffleWorkerRegistrationSuccess) shuffleManager.registerWorker(worker).get();

        DataPartitionStatus dataPartitionStatus =
                new DataPartitionStatus(
                        RandomIDUtils.randomJobId(),
                        new DataPartitionCoordinate(
                                RandomIDUtils.randomDataSetId(),
                                RandomIDUtils.randomMapPartitionId()));
        DataPartitionStatus releasingDataPartitionStatus =
                new DataPartitionStatus(
                        RandomIDUtils.randomJobId(),
                        new DataPartitionCoordinate(
                                RandomIDUtils.randomDataSetId(),
                                RandomIDUtils.randomMapPartitionId()));

        shuffleManager.heartbeatFromWorker(
                worker.getWorkerID(),
                new WorkerToManagerHeartbeatPayload(
                        Arrays.asList(dataPartitionStatus, releasingDataPartitionStatus), 0, 0));

        assertTrue(
                shuffleManager
                        .getAssignmentTracker()
                        .isJobRegistered(dataPartitionStatus.getJobId()));
        assertTrue(
                shuffleManager
                        .getAssignmentTracker()
                        .isJobRegistered(releasingDataPartitionStatus.getJobId()));
    }

    private void assertFailedWithInconsistentInstanceId(CompletableFuture<?> result) {
        try {
            result.get();
        } catch (Exception e) {
            assertTrue(ExceptionUtils.findThrowable(e, IllegalStateException.class).isPresent());
        }
    }

    private ShuffleWorkerGateway createShuffleWorkerGateway() {
        final ShuffleWorkerGateway shuffleWorkerGateway = new TestShuffleWorkerGateway();
        rpcService.registerGateway(shuffleWorkerGateway.getAddress(), shuffleWorkerGateway);
        return shuffleWorkerGateway;
    }

    private ShuffleManagerGateway initializeServicesAndShuffleManagerGateway()
            throws InterruptedException, ExecutionException, TimeoutException {
        testingFatalErrorHandler = new TestingFatalErrorHandler();
        InstanceID shuffleManagerInstanceID = new InstanceID();
        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        testAssignmentTracker = new AssignmentTrackerImpl(configuration);

        leaderElectionService = new TestingLeaderElectionService();
        final TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        highAvailabilityServices.setShuffleManagerLeaderElectionService(leaderElectionService);
        final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, HEARTBEAT_TIMEOUT);
        shuffleManager =
                new ShuffleManager(
                        rpcService,
                        shuffleManagerInstanceID,
                        highAvailabilityServices,
                        testingFatalErrorHandler,
                        ForkJoinPool.commonPool(),
                        heartbeatServices,
                        heartbeatServices,
                        testAssignmentTracker);

        shuffleManager.start();

        wronglyFencedGateway =
                rpcService
                        .connectTo(
                                shuffleManager.getAddress(),
                                UUID.randomUUID(),
                                ShuffleManagerGateway.class)
                        .get(TIMEOUT, TimeUnit.MILLISECONDS);
        grantLeadership(leaderElectionService).get(TIMEOUT, TimeUnit.MILLISECONDS);
        return shuffleManager.getSelfGateway(ShuffleManagerGateway.class);
    }

    private CompletableFuture<UUID> grantLeadership(
            TestingLeaderElectionService leaderElectionService) {
        UUID leaderSessionId = UUID.randomUUID();
        return leaderElectionService.isLeader(leaderSessionId);
    }

    private DataPartitionStatus createDataPartitionStatus() {
        final DataPartitionCoordinate dataPartitionCoordinate =
                new DataPartitionCoordinate(
                        RandomIDUtils.randomDataSetId(), RandomIDUtils.randomMapPartitionId());
        final DataPartitionStatus dataPartitionStatus =
                new DataPartitionStatus(RandomIDUtils.randomJobId(), dataPartitionCoordinate);
        return dataPartitionStatus;
    }

    private static class TestShuffleWorkerGateway implements ShuffleWorkerGateway {

        private static final String rpcAddress = "foobar:1234";

        private static final String hostName = "foobar";

        private static final int dataPort = 1234;

        private static final InstanceID shuffleWorkerID = new InstanceID();

        private static final int processId = 12345;

        static ShuffleWorkerRegistration createShuffleWorkerRegistration() {
            return new ShuffleWorkerRegistration(
                    rpcAddress, hostName, shuffleWorkerID, dataPort, processId);
        }

        static ShuffleWorkerRegistration createShuffleWorkerRegistration(final String rpcAddress) {
            return new ShuffleWorkerRegistration(
                    rpcAddress, hostName, shuffleWorkerID, dataPort, processId);
        }

        static InstanceID getShuffleWorkerID() {
            return shuffleWorkerID;
        }

        @Override
        public void heartbeatFromManager(InstanceID managerID) {}

        @Override
        public void disconnectManager(Exception cause) {}

        @Override
        public CompletableFuture<Acknowledge> releaseDataPartition(
                JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
            return null;
        }

        @Override
        public CompletableFuture<Acknowledge> removeReleasedDataPartitionMeta(
                JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
            return null;
        }

        @Override
        public CompletableFuture<ShuffleWorkerMetrics> getWorkerMetrics() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public String getAddress() {
            return rpcAddress;
        }

        @Override
        public String getHostname() {
            return null;
        }
    }
}
