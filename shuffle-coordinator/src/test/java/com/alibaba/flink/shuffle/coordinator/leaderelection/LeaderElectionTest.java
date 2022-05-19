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
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderContender;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.embeded.EmbeddedLeaderService;
import com.alibaba.flink.shuffle.coordinator.highavailability.standalone.StandaloneLeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperHaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperUtils;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for leader election. */
@RunWith(Parameterized.class)
public class LeaderElectionTest extends TestLogger {

    enum LeaderElectionType {
        ZooKeeper,
        Embedded,
        Standalone
    }

    @Parameterized.Parameters(name = "Leader election: {0}")
    public static Collection<LeaderElectionType> parameters() {
        return Arrays.asList(LeaderElectionType.values());
    }

    private final ServiceClass serviceClass;

    public LeaderElectionTest(LeaderElectionType leaderElectionType) {
        switch (leaderElectionType) {
            case ZooKeeper:
                serviceClass = new ZooKeeperServiceClass();
                break;
            case Embedded:
                serviceClass = new EmbeddedServiceClass();
                break;
            case Standalone:
                serviceClass = new StandaloneServiceClass();
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Unknown leader election type: %s.", leaderElectionType));
        }
    }

    @Before
    public void setup() throws Exception {
        serviceClass.setup();
    }

    @After
    public void teardown() throws Exception {
        serviceClass.teardown();
    }

    @Test
    public void testHasLeadership() throws Exception {
        final LeaderElectionService leaderElectionService =
                serviceClass.createLeaderElectionService();
        final ManualLeaderContender manualLeaderContender = new ManualLeaderContender();

        try {
            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()), is(false));

            leaderElectionService.start(manualLeaderContender);

            final UUID leaderSessionId = manualLeaderContender.waitForLeaderSessionId();

            assertThat(leaderElectionService.hasLeadership(leaderSessionId), is(true));
            assertThat(leaderElectionService.hasLeadership(UUID.randomUUID()), is(false));

            leaderElectionService.confirmLeadership(
                    new LeaderInformation(leaderSessionId, "foobar"));

            assertThat(leaderElectionService.hasLeadership(leaderSessionId), is(true));

            leaderElectionService.stop();

            assertThat(leaderElectionService.hasLeadership(leaderSessionId), is(false));
        } finally {
            manualLeaderContender.rethrowError();
        }
    }

    private static final class ManualLeaderContender implements LeaderContender {

        private static final UUID NULL_LEADER_SESSION_ID = new UUID(0L, 0L);

        private final ArrayBlockingQueue<UUID> leaderSessionIds = new ArrayBlockingQueue<>(10);

        private volatile Throwable throwable;

        @Override
        public void grantLeadership(UUID leaderSessionID) {
            leaderSessionIds.offer(leaderSessionID);
        }

        @Override
        public void revokeLeadership() {
            leaderSessionIds.offer(NULL_LEADER_SESSION_ID);
        }

        @Override
        public String getDescription() {
            return "foobar";
        }

        @Override
        public void handleError(Throwable throwable) {
            this.throwable = throwable;
        }

        void rethrowError() throws Exception {
            if (throwable != null) {
                ExceptionUtils.rethrowException(throwable);
            }
        }

        UUID waitForLeaderSessionId() throws InterruptedException {
            return leaderSessionIds.take();
        }
    }

    private interface ServiceClass {
        void setup() throws Exception;

        void teardown() throws Exception;

        LeaderElectionService createLeaderElectionService() throws Exception;
    }

    private static final class ZooKeeperServiceClass implements ServiceClass {

        private TestingServer testingServer;

        private CuratorFramework client;

        private Configuration configuration;

        @Override
        public void setup() throws Exception {
            try {
                testingServer = new TestingServer();
            } catch (Exception e) {
                throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
            }

            configuration = new Configuration();

            configuration.setString(
                    HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
            configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

            client = ZooKeeperUtils.startCuratorFramework(configuration);
        }

        @Override
        public void teardown() throws Exception {
            if (client != null) {
                client.close();
                client = null;
            }

            if (testingServer != null) {
                testingServer.stop();
                testingServer = null;
            }
        }

        @Override
        public LeaderElectionService createLeaderElectionService() throws Exception {
            return ZooKeeperUtils.createLeaderElectionService(
                    client,
                    configuration,
                    ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_LATCH_PATH,
                    ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH);
        }
    }

    private static final class EmbeddedServiceClass implements ServiceClass {

        private EmbeddedLeaderService embeddedLeaderService;

        private ExecutorService executor;

        @Override
        public void setup() {
            executor = Executors.newSingleThreadExecutor();
            embeddedLeaderService = new EmbeddedLeaderService(executor);
        }

        @Override
        public void teardown() {
            if (embeddedLeaderService != null) {
                embeddedLeaderService.shutdown();
                embeddedLeaderService = null;
            }

            if (executor != null) {
                executor.shutdown();
                executor = null;
            }
        }

        @Override
        public LeaderElectionService createLeaderElectionService() throws Exception {
            return embeddedLeaderService.createLeaderElectionService();
        }
    }

    private static final class StandaloneServiceClass implements ServiceClass {

        @Override
        public void setup() throws Exception {
            // noop
        }

        @Override
        public void teardown() throws Exception {
            // noop
        }

        @Override
        public LeaderElectionService createLeaderElectionService() throws Exception {
            return new StandaloneLeaderElectionService();
        }
    }
}
