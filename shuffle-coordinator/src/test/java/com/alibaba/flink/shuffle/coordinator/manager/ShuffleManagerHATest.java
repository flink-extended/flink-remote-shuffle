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

import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.leaderelection.TestingLeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.AssignmentTracker;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.ChangedWorkerStatus;
import com.alibaba.flink.shuffle.coordinator.utils.TestingFatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.UsableStorageSpaceInfo;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.test.TestingRpcService;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/** Tests for the ShuffleManager HA. */
public class ShuffleManagerHATest {

    @Test
    public void testGrantAndRevokeLeadership() throws Exception {
        final InstanceID rmInstanceID = new InstanceID();
        final RemoteShuffleRpcService rpcService = new TestingRpcService();

        final CompletableFuture<UUID> leaderSessionIdFuture = new CompletableFuture<>();

        final TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService() {
                    @Override
                    public void confirmLeadership(LeaderInformation leaderInfo) {
                        leaderSessionIdFuture.complete(leaderInfo.getLeaderSessionID());
                    }
                };

        final HaServices highAvailabilityServices =
                new TestHaService() {
                    @Override
                    public LeaderElectionService createLeaderElectionService() {
                        return leaderElectionService;
                    }
                };

        final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

        final CompletableFuture<UUID> revokedLeaderIdFuture = new CompletableFuture<>();

        final ShuffleManager shuffleManager =
                new ShuffleManager(
                        rpcService,
                        rmInstanceID,
                        highAvailabilityServices,
                        testingFatalErrorHandler,
                        ForkJoinPool.commonPool(),
                        new HeartbeatServices(100, 200),
                        new HeartbeatServices(100, 200),
                        new TestAssignmentTracker()) {

                    @Override
                    public void revokeLeadership() {
                        super.revokeLeadership();
                        runAsyncWithoutFencing(
                                () -> revokedLeaderIdFuture.complete(getFencingToken()));
                    }
                };

        try {
            shuffleManager.start();

            Assert.assertNull(shuffleManager.getFencingToken());
            final UUID leaderId = UUID.randomUUID();
            leaderElectionService.isLeader(leaderId);
            // after grant leadership, ShuffleManager's leaderId has value
            Assert.assertEquals(leaderId, leaderSessionIdFuture.get());
            // then revoke leadership, ShuffleManager's leaderId should be different
            leaderElectionService.notLeader();
            Assert.assertNotEquals(leaderId, revokedLeaderIdFuture.get());

            if (testingFatalErrorHandler.hasExceptionOccurred()) {
                testingFatalErrorHandler.rethrowError();
            }
        } finally {
            rpcService.stopService().get();
        }
    }

    private static class TestHaService implements HaServices {

        @Override
        public LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor) {
            return null;
        }

        @Override
        public LeaderElectionService createLeaderElectionService() {
            return null;
        }

        @Override
        public void closeAndCleanupAllData() throws Exception {}

        @Override
        public void close() throws Exception {}
    }

    private static class TestAssignmentTracker implements AssignmentTracker {

        @Override
        public boolean isWorkerRegistered(RegistrationID registrationID) {
            return false;
        }

        @Override
        public void registerWorker(
                InstanceID workerID,
                RegistrationID registrationID,
                ShuffleWorkerGateway gateway,
                String externalAddress,
                int dataPort) {}

        @Override
        public void workerReportDataPartitionReleased(
                RegistrationID registrationID,
                JobID jobID,
                DataSetID dataSetID,
                DataPartitionID dataPartitionID) {}

        @Override
        public void synchronizeWorkerDataPartitions(
                RegistrationID registrationID, List<DataPartitionStatus> dataPartitionStatuses) {}

        @Override
        public void unregisterWorker(RegistrationID registrationID) {}

        @Override
        public boolean isJobRegistered(JobID jobID) {
            return false;
        }

        @Override
        public void registerJob(JobID jobID) {}

        @Override
        public void unregisterJob(JobID jobID) {}

        @Override
        public ShuffleResource requestShuffleResource(
                JobID jobID,
                DataSetID dataSetID,
                MapPartitionID mapPartitionID,
                int numberOfConsumers,
                String dataPartitionFactoryName) {
            return null;
        }

        @Override
        public void releaseShuffleResource(
                JobID jobID, DataSetID dataSetID, MapPartitionID mapPartitionID) {}

        @Override
        public ChangedWorkerStatus computeChangedWorkers(
                JobID jobID,
                Collection<InstanceID> cachedWorkerList,
                boolean considerUnrelatedWorkers) {
            return null;
        }

        @Override
        public List<JobID> listJobs() {
            return null;
        }

        @Override
        public int getNumberOfWorkers() {
            return 0;
        }

        @Override
        public void reportWorkerStorageSpaces(
                InstanceID instanceID,
                RegistrationID shuffleWorkerRegisterId,
                Map<String, UsableStorageSpaceInfo> usableSpace) {}

        @Override
        public Map<DataPartitionCoordinate, InstanceID> getDataPartitionDistribution(JobID jobID) {
            return null;
        }
    }
}
