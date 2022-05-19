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

package com.alibaba.flink.shuffle.coordinator.leaderelection;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.DirectlyFailingFatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalEventHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperHaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperMultiLeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperSingleLeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperUtils;
import com.alibaba.flink.shuffle.coordinator.utils.CommonTestUtils;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Tests for the error handling in case of a suspended connection to the ZooKeeper instance. */
@RunWith(Parameterized.class)
public class ZooKeeperLeaderElectionConnectionHandlingTest extends TestLogger {

    private TestingServer testingServer;

    private CuratorFramework zooKeeperClient;

    private final FatalErrorHandler fatalErrorHandler = DirectlyFailingFatalErrorHandler.INSTANCE;

    public final HaServices.LeaderReceptor leaderReceptor;

    private final String retrievalPath =
            ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.defaultValue()
                    + ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH;

    @Parameterized.Parameters(name = "leader receptor ={0}")
    public static Object[] parameter() {
        return HaServices.LeaderReceptor.values();
    }

    public ZooKeeperLeaderElectionConnectionHandlingTest(HaServices.LeaderReceptor leaderReceptor) {
        this.leaderReceptor = leaderReceptor;
    }

    @Before
    public void before() throws Exception {
        testingServer = new TestingServer();

        Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
        config.setDuration(
                HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, Duration.ofSeconds(10));

        zooKeeperClient = ZooKeeperUtils.startCuratorFramework(config);
    }

    @After
    public void after() throws Exception {
        closeTestServer();

        if (zooKeeperClient != null) {
            zooKeeperClient.close();
            zooKeeperClient = null;
        }
    }

    @Test
    public void testConnectionSuspendedWhenMultipleLeaderSelection() throws Exception {
        String retrievalPathPrefix = "/cluster-";
        int numLeaders = 10;
        int leaderIndex = 5;
        for (int i = 0; i < numLeaders; ++i) {
            LeaderInformation leaderInfo =
                    new LeaderInformation(
                            i,
                            i <= leaderIndex ? 0 : leaderIndex,
                            UUID.randomUUID(),
                            "test address " + i);
            writeLeaderInformationToZooKeeper(
                    retrievalPathPrefix
                            + i
                            + ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH,
                    leaderInfo);
        }

        Thread.sleep(2000);

        QueueLeaderListener leaderListener = new QueueLeaderListener(10);
        try (LeaderRetrievalDriver ignored =
                createLeaderRetrievalDriver(
                        HaServices.LeaderReceptor.SHUFFLE_CLIENT,
                        ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH,
                        retrievalPathPrefix,
                        leaderListener)) {
            assertEquals(
                    "test address " + leaderIndex, leaderListener.next().get().getLeaderAddress());

            testingServer.stop();
            // connection suspended
            assertEquals(LeaderInformation.empty(), leaderListener.next().get());

            testingServer.restart();
            LeaderInformation leaderInfo = leaderListener.next().get();
            if (leaderInfo.isEmpty()) {
                leaderInfo = leaderListener.next().get();
            }
            assertEquals("test address " + leaderIndex, leaderInfo.getLeaderAddress());

            testingServer.stop();
            // connection lost
            assertEquals(LeaderInformation.empty(), leaderListener.next().get());
            assertEquals(LeaderInformation.empty(), leaderListener.next().get());

            testingServer.restart();
            assertEquals(
                    "test address " + leaderIndex, leaderListener.next().get().getLeaderAddress());
        }
    }

    @Test
    public void testConnectionSuspendedHandlingDuringInitialization() throws Exception {
        final QueueLeaderListener leaderListener = new QueueLeaderListener(1);
        try (LeaderRetrievalDriver ignored = createLeaderRetrievalDriver(leaderListener)) {

            // do the testing
            final CompletableFuture<LeaderInformation> firstAddress =
                    leaderListener.next(Duration.ofMillis(50));
            assertNull(firstAddress);

            closeTestServer();

            // QueueLeaderElectionListener will be notified with an empty leader when ZK connection
            // is suspended
            final CompletableFuture<LeaderInformation> secondAddress = leaderListener.next();
            assertNotNull(secondAddress);
            assertEquals(LeaderInformation.empty(), secondAddress.get());
        }
    }

    @Test
    public void testConnectionSuspendedHandling() throws Exception {
        final String leaderAddress = "localhost";
        final QueueLeaderListener leaderListener = new QueueLeaderListener(1);
        try (LeaderRetrievalDriver ignored = createLeaderRetrievalDriver(leaderListener)) {

            LeaderInformation leaderInfo = new LeaderInformation(UUID.randomUUID(), leaderAddress);
            writeLeaderInformationToZooKeeper(leaderInfo);

            // do the testing
            CompletableFuture<LeaderInformation> firstAddress = leaderListener.next();
            assertEquals(leaderInfo, firstAddress.get());

            closeTestServer();

            CompletableFuture<LeaderInformation> secondAddress = leaderListener.next();
            assertNotNull(secondAddress);
            assertEquals(LeaderInformation.empty(), secondAddress.get());
        }
    }

    @Test
    public void testSameLeaderAfterReconnectTriggersListenerNotification() throws Exception {
        final QueueLeaderListener leaderListener = new QueueLeaderListener(1);
        try (LeaderRetrievalDriver ignored = createLeaderRetrievalDriver(leaderListener)) {

            String leaderAddress = "foobar";
            UUID sessionId = UUID.randomUUID();
            LeaderInformation leaderInfo = new LeaderInformation(sessionId, leaderAddress);
            writeLeaderInformationToZooKeeper(leaderInfo);

            // pop new leader
            leaderListener.next();

            testingServer.stop();

            final CompletableFuture<LeaderInformation> connectionSuspension = leaderListener.next();

            // wait until the ZK connection is suspended
            connectionSuspension.join();

            testingServer.restart();

            // new old leader information should be announced
            final CompletableFuture<LeaderInformation> connectionReconnect = leaderListener.next();
            assertEquals(leaderInfo, connectionReconnect.get());
        }
    }

    private void writeLeaderInformationToZooKeeper(LeaderInformation leaderInfo) throws Exception {
        writeLeaderInformationToZooKeeper(retrievalPath, leaderInfo);
    }

    private void writeLeaderInformationToZooKeeper(
            String retrievalPath, LeaderInformation leaderInfo) throws Exception {
        final byte[] data = leaderInfo.toByteArray();
        if (zooKeeperClient.checkExists().forPath(retrievalPath) != null) {
            zooKeeperClient.setData().forPath(retrievalPath, data);
        } else {
            zooKeeperClient.create().creatingParentsIfNeeded().forPath(retrievalPath, data);
        }
    }

    @Test
    public void testNewLeaderAfterReconnectTriggersListenerNotification() throws Exception {
        final QueueLeaderListener leaderListener = new QueueLeaderListener(1);

        try (LeaderRetrievalDriver ignored = createLeaderRetrievalDriver(leaderListener)) {

            final String leaderAddress = "foobar";
            final UUID sessionId = UUID.randomUUID();
            writeLeaderInformationToZooKeeper(new LeaderInformation(sessionId, leaderAddress));

            // pop new leader
            leaderListener.next();

            testingServer.stop();

            final CompletableFuture<LeaderInformation> connectionSuspension = leaderListener.next();

            // wait until the ZK connection is suspended
            connectionSuspension.join();

            testingServer.restart();

            String newLeaderAddress = "barfoo";
            UUID newSessionId = UUID.randomUUID();
            LeaderInformation leaderInfo = new LeaderInformation(newSessionId, newLeaderAddress);
            writeLeaderInformationToZooKeeper(leaderInfo);

            // check that we find the new leader information eventually
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        final CompletableFuture<LeaderInformation> afterConnectionReconnect =
                                leaderListener.next();
                        return afterConnectionReconnect.get().equals(leaderInfo);
                    },
                    30L * 1000);
        }
    }

    private LeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderListener) throws Exception {
        return createLeaderRetrievalDriver(
                leaderReceptor,
                retrievalPath,
                ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.defaultValue(),
                leaderListener);
    }

    private LeaderRetrievalDriver createLeaderRetrievalDriver(
            HaServices.LeaderReceptor leaderReceptor,
            String retrievalPath,
            String retrievalPathPrefix,
            LeaderRetrievalEventHandler leaderListener)
            throws Exception {
        switch (leaderReceptor) {
            case SHUFFLE_WORKER:
                return new ZooKeeperSingleLeaderRetrievalDriver(
                        zooKeeperClient, retrievalPath, leaderListener, fatalErrorHandler);
            case SHUFFLE_CLIENT:
                return new ZooKeeperMultiLeaderRetrievalDriver(
                        zooKeeperClient,
                        retrievalPath,
                        retrievalPathPrefix,
                        leaderListener,
                        fatalErrorHandler);
            default:
                throw new Exception("Unknown leader receptor type: " + leaderReceptor);
        }
    }

    private void closeTestServer() throws IOException {
        if (testingServer != null) {
            testingServer.close();
            testingServer = null;
        }
    }

    private static class QueueLeaderListener implements LeaderRetrievalEventHandler {

        private final BlockingQueue<CompletableFuture<LeaderInformation>> queue;

        public QueueLeaderListener(int expectedCalls) {
            this.queue = new ArrayBlockingQueue<>(expectedCalls);
        }

        @Override
        public void notifyLeaderAddress(LeaderInformation leaderInfo) {
            try {
                queue.put(CompletableFuture.completedFuture(leaderInfo));
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

        public CompletableFuture<LeaderInformation> next() {
            return next(null);
        }

        public CompletableFuture<LeaderInformation> next(@Nullable Duration timeout) {
            try {
                if (timeout == null) {
                    return queue.take();
                } else {
                    return this.queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
