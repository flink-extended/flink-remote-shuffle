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

package com.alibaba.flink.shuffle.coordinator.leaderelection;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperHaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperUtils;
import com.alibaba.flink.shuffle.coordinator.utils.LeaderRetrievalUtils;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.utils.TestLogger;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests for the ZooKeeper based leader election and retrieval. */
@RunWith(Parameterized.class)
public class ZooKeeperLeaderRetrievalTest extends TestLogger {

    private TestingServer testingServer;

    private HaServices haServices;

    public final HaServices.LeaderReceptor leaderReceptor;

    @Parameterized.Parameters(name = "leader receptor ={0}")
    public static Object[] parameter() {
        return HaServices.LeaderReceptor.values();
    }

    public ZooKeeperLeaderRetrievalTest(HaServices.LeaderReceptor leaderReceptor) {
        this.leaderReceptor = leaderReceptor;
    }

    @Before
    public void before() throws Exception {
        testingServer = new TestingServer();
        haServices = createHaService(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.defaultValue());
        AkkaRpcServiceUtils.loadRpcSystem(new Configuration());
    }

    @After
    public void after() throws Exception {
        if (haServices != null) {
            haServices.closeAndCleanupAllData();

            haServices = null;
        }

        if (testingServer != null) {
            testingServer.stop();

            testingServer = null;
        }

        AkkaRpcServiceUtils.closeRpcSystem();
    }

    private HaServices createHaService(String clusterID) {
        Configuration config = new Configuration();
        return new ZooKeeperHaServices(config, createZooKeeperClient(config, clusterID));
    }

    private CuratorFramework createZooKeeperClient(Configuration config, String clusterID) {
        config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
        config.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, clusterID);
        return ZooKeeperUtils.startCuratorFramework(config);
    }

    @Test
    public void testLeaderRetrievalAfterLeaderElection() throws Exception {
        LeaderElectionService leaderElectionService = haServices.createLeaderElectionService();
        String address = "test leader address";
        TestingContender testingContender = new TestingContender(address, leaderElectionService);
        leaderElectionService.start(testingContender);

        LeaderRetrievalService leaderRetrievalService =
                haServices.createLeaderRetrievalService(leaderReceptor);
        TestingListener testingListener = new TestingListener();
        leaderRetrievalService.start(testingListener);

        assertNotNull(address, testingListener.waitForNewLeader(60000));

        leaderElectionService.stop();
        leaderRetrievalService.stop();
    }

    @Test
    public void testLeaderRetrievalBeforeLeaderElection() throws Exception {
        LeaderRetrievalService leaderRetrievalService =
                haServices.createLeaderRetrievalService(leaderReceptor);
        TestingListener testingListener = new TestingListener();
        leaderRetrievalService.start(testingListener);

        LeaderElectionService leaderElectionService = haServices.createLeaderElectionService();
        String address = "test leader address";
        TestingContender testingContender = new TestingContender(address, leaderElectionService);
        leaderElectionService.start(testingContender);

        assertNotNull(address, testingListener.waitForNewLeader(60000));

        leaderElectionService.stop();
        leaderRetrievalService.stop();
    }

    @Test
    public void testLeaderChange() throws Exception {
        LeaderElectionService leaderElectionService1 = haServices.createLeaderElectionService();
        String address1 = "test leader address1";
        TestingContender testingContender1 = new TestingContender(address1, leaderElectionService1);
        leaderElectionService1.start(testingContender1);

        LeaderRetrievalService leaderRetrievalService =
                haServices.createLeaderRetrievalService(leaderReceptor);
        TestingListener testingListener = new TestingListener();
        leaderRetrievalService.start(testingListener);

        assertNotNull(address1, testingListener.waitForNewLeader(60000));

        LeaderElectionService leaderElectionService2 = haServices.createLeaderElectionService();
        String address2 = "test leader address2";
        TestingContender testingContender2 = new TestingContender(address2, leaderElectionService2);
        leaderElectionService2.start(testingContender2);

        leaderElectionService1.stop();
        assertNotNull(address2, testingListener.waitForNewLeader(60000));
    }

    private void writeLeaderInformationToZooKeeper(
            CuratorFramework client, String retrievalPath, LeaderInformation leaderInfo)
            throws Exception {
        final byte[] data = leaderInfo.toByteArray();
        if (client.checkExists().forPath(retrievalPath) != null) {
            client.setData().forPath(retrievalPath, data);
        } else {
            client.create().creatingParentsIfNeeded().forPath(retrievalPath, data);
        }
    }

    @Test
    public void testMultipleLeaderSelection() throws Exception {
        int numLeaders = 10;
        int leaderIndex = 5;
        CuratorFramework client = createZooKeeperClient(new Configuration(), "ignored");
        for (int i = 0; i < numLeaders; ++i) {
            LeaderInformation leaderInfo =
                    new LeaderInformation(
                            i,
                            i <= leaderIndex ? 0 : leaderIndex,
                            UUID.randomUUID(),
                            "test address " + i);
            writeLeaderInformationToZooKeeper(
                    client,
                    "/cluster-" + i + ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH,
                    leaderInfo);
        }

        Thread.sleep(2000);

        LeaderRetrievalService leaderRetrievalService =
                haServices.createLeaderRetrievalService(HaServices.LeaderReceptor.SHUFFLE_CLIENT);
        TestingListener testingListener = new TestingListener();
        leaderRetrievalService.start(testingListener);
        assertEquals("test address " + leaderIndex, testingListener.waitForNewLeader(60000));

        String leaderPath =
                "/cluster-"
                        + leaderIndex
                        + ZooKeeperHaServices.SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH;
        LeaderInformation newLeaderInfo = new LeaderInformation(UUID.randomUUID(), "mew address");
        client.setData().forPath(leaderPath, newLeaderInfo.toByteArray());
        assertEquals("mew address", testingListener.waitForNewLeader(60000));

        // remove the leader node
        client.delete().forPath(leaderPath);
        assertEquals("test address " + (leaderIndex - 1), testingListener.waitForNewLeader(60000));

        leaderRetrievalService.stop();
        client.close();
    }

    /**
     * Tests that LeaderRetrievalUtils.findConnectingAddress finds the correct connecting address in
     * case of an old leader address in ZooKeeper and a subsequent election of a new leader. The
     * findConnectingAddress should block until the new leader has been elected and his address has
     * been written to ZooKeeper.
     */
    @Test
    public void testConnectingAddressRetrievalWithDelayedLeaderElection() throws Exception {
        Duration timeout = Duration.ofMinutes(1L);

        long sleepingTime = 1000;

        LeaderElectionService leaderElectionService = null;
        LeaderElectionService faultyLeaderElectionService;

        ServerSocket serverSocket;
        InetAddress localHost;

        Thread thread;

        try {
            String wrongAddress =
                    AkkaRpcServiceUtils.getRpcUrl(
                            "1.1.1.1", 1234, "foobar", AkkaRpcServiceUtils.AkkaProtocol.TCP);

            try {
                localHost = InetAddress.getLocalHost();
                serverSocket = new ServerSocket(0, 50, localHost);
            } catch (UnknownHostException e) {
                // may happen if disconnected. skip test.
                System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
                return;
            } catch (IOException e) {
                // may happen in certain test setups, skip test.
                System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
                return;
            }

            InetSocketAddress correctInetSocketAddress =
                    new InetSocketAddress(localHost, serverSocket.getLocalPort());

            String correctAddress =
                    AkkaRpcServiceUtils.getRpcUrl(
                            localHost.getHostName(),
                            correctInetSocketAddress.getPort(),
                            "Test",
                            AkkaRpcServiceUtils.AkkaProtocol.TCP);

            faultyLeaderElectionService = haServices.createLeaderElectionService();
            TestingContender wrongLeaderAddressContender =
                    new TestingContender(wrongAddress, faultyLeaderElectionService);

            faultyLeaderElectionService.start(wrongLeaderAddressContender);

            FindConnectingAddress findConnectingAddress =
                    new FindConnectingAddress(
                            timeout, haServices.createLeaderRetrievalService(leaderReceptor));

            thread = new Thread(findConnectingAddress);

            thread.start();

            leaderElectionService = haServices.createLeaderElectionService();
            TestingContender correctLeaderAddressContender =
                    new TestingContender(correctAddress, leaderElectionService);

            Thread.sleep(sleepingTime);

            faultyLeaderElectionService.stop();

            leaderElectionService.start(correctLeaderAddressContender);

            thread.join();

            InetAddress result = findConnectingAddress.getInetAddress();

            // check that we can connect to the localHost
            Socket socket = new Socket();
            try {
                // port 0 = let the OS choose the port
                SocketAddress bindP = new InetSocketAddress(result, 0);
                // machine
                socket.bind(bindP);
                socket.connect(correctInetSocketAddress, 1000);
            } finally {
                socket.close();
            }
        } finally {
            if (leaderElectionService != null) {
                leaderElectionService.stop();
            }
        }
    }

    /**
     * Tests that the LeaderRetrievalUtils.findConnectingAddress stops trying to find the connecting
     * address if no leader address has been specified. The call should return then
     * InetAddress.getLocalHost().
     */
    @Test
    public void testTimeoutOfFindConnectingAddress() throws Exception {
        Duration timeout = Duration.ofSeconds(1L);

        LeaderRetrievalService leaderRetrievalService =
                haServices.createLeaderRetrievalService(leaderReceptor);
        InetAddress result =
                LeaderRetrievalUtils.findConnectingAddress(leaderRetrievalService, timeout);

        assertEquals(InetAddress.getLocalHost(), result);
    }

    static class FindConnectingAddress implements Runnable {

        private final Duration timeout;
        private final LeaderRetrievalService leaderRetrievalService;

        private InetAddress result;
        private Exception exception;

        public FindConnectingAddress(
                Duration timeout, LeaderRetrievalService leaderRetrievalService) {
            this.timeout = timeout;
            this.leaderRetrievalService = leaderRetrievalService;
        }

        @Override
        public void run() {
            try {
                result =
                        LeaderRetrievalUtils.findConnectingAddress(leaderRetrievalService, timeout);
            } catch (Exception e) {
                exception = e;
            }
        }

        public InetAddress getInetAddress() throws Exception {
            if (exception != null) {
                throw exception;
            } else {
                return result;
            }
        }
    }
}
