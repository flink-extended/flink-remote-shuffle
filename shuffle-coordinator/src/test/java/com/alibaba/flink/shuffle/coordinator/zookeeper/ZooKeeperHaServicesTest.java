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

package com.alibaba.flink.shuffle.coordinator.zookeeper;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.functions.ConsumerWithException;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaMode;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServiceUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperHaServices;
import com.alibaba.flink.shuffle.coordinator.leaderelection.TestingContender;
import com.alibaba.flink.shuffle.coordinator.leaderelection.TestingListener;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator4.org.apache.curator.retry.RetryNTimes;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/** Tests for the {@link ZooKeeperHaServices}. */
public class ZooKeeperHaServicesTest extends TestLogger {

    @ClassRule public static final ZooKeeperResource ZOO_KEEPER_RESOURCE = new ZooKeeperResource();

    private static CuratorFramework client;

    @BeforeClass
    public static void setupClass() {
        client = startCuratorFramework();
        client.start();
    }

    @Before
    public void setup() throws Exception {
        final List<String> children = client.getChildren().forPath("/");

        for (String child : children) {
            if (!child.equals("zookeeper")) {
                client.delete().deletingChildrenIfNeeded().forPath('/' + child);
            }
        }
    }

    @AfterClass
    public static void teardownClass() {
        if (client != null) {
            client.close();
        }
    }

    /** Tests that a simple {@link ZooKeeperHaServices#close()} does not delete Zookeeper paths. */
    @Test
    public void testSimpleClose() throws Exception {
        final String rootPath = "/foo/bar/flink";
        final Configuration configuration = createConfiguration(rootPath);

        runCleanupTest(configuration, ZooKeeperHaServices::close);

        final List<String> children = client.getChildren().forPath(rootPath);
        assertThat(children, is(not(empty())));
    }

    /**
     * Tests that the {@link ZooKeeperHaServices} cleans up all paths if it is closed via {@link
     * ZooKeeperHaServices#closeAndCleanupAllData()}.
     */
    @Test
    public void testSimpleCloseAndCleanupAllData() throws Exception {
        final Configuration configuration = createConfiguration("/foo/bar/flink");

        final List<String> initialChildren = client.getChildren().forPath("/");

        runCleanupTest(configuration, ZooKeeperHaServices::closeAndCleanupAllData);

        final List<String> children = client.getChildren().forPath("/");
        assertThat(children, is(equalTo(initialChildren)));
    }

    /** Tests that we can only delete the parent znodes as long as they are empty. */
    @Test
    public void testCloseAndCleanupAllDataWithUncle() throws Exception {
        final String prefix = "/foo/bar";
        final String flinkPath = prefix + "/flink";
        final Configuration configuration = createConfiguration(flinkPath);

        final String unclePath = prefix + "/foobar";
        client.create().creatingParentContainersIfNeeded().forPath(unclePath);

        runCleanupTest(configuration, ZooKeeperHaServices::closeAndCleanupAllData);

        assertThat(client.checkExists().forPath(flinkPath), is(nullValue()));
        assertThat(client.checkExists().forPath(unclePath), is(notNullValue()));
    }

    private static CuratorFramework startCuratorFramework() {
        return CuratorFrameworkFactory.builder()
                .connectString(ZOO_KEEPER_RESOURCE.getConnectString())
                .retryPolicy(new RetryNTimes(50, 100))
                .build();
    }

    @Nonnull
    private Configuration createConfiguration(String rootPath) {
        final Configuration configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOO_KEEPER_RESOURCE.getConnectString());
        configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, rootPath);
        return configuration;
    }

    private void runCleanupTest(
            Configuration configuration,
            ConsumerWithException<ZooKeeperHaServices, Exception> zooKeeperHaServicesConsumer)
            throws Exception {
        configuration.setString(HighAvailabilityOptions.HA_MODE, HaMode.ZOOKEEPER.toString());
        try (ZooKeeperHaServices zooKeeperHaServices =
                (ZooKeeperHaServices)
                        HaServiceUtils.createAvailableOrEmbeddedServices(
                                configuration, Runnable::run)) {

            // create some Zk services to trigger the generation of paths
            final LeaderRetrievalService shuffleManagerLeaderRetriever =
                    zooKeeperHaServices.createLeaderRetrievalService(
                            HaServices.LeaderReceptor.SHUFFLE_WORKER);
            final LeaderElectionService shuffleManagerLeaderElectionService =
                    zooKeeperHaServices.createLeaderElectionService();

            final TestingListener listener = new TestingListener();
            shuffleManagerLeaderRetriever.start(listener);
            shuffleManagerLeaderElectionService.start(
                    new TestingContender("foobar", shuffleManagerLeaderElectionService));

            listener.waitForNewLeader(2000L);

            shuffleManagerLeaderRetriever.stop();
            shuffleManagerLeaderElectionService.stop();

            zooKeeperHaServicesConsumer.accept(zooKeeperHaServices);
        }
    }
}
