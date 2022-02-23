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

package com.alibaba.flink.shuffle.e2e;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperHaServices;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.HeartbeatOptions;
import com.alibaba.flink.shuffle.e2e.flinkcluster.FlinkLocalCluster;
import com.alibaba.flink.shuffle.e2e.shufflecluster.LocalShuffleCluster;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestEnvironment;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestUtils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.WebOptions;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** Shuffle manager ha test. */
public class ShuffleManagerHAE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerHAE2ETest.class);

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public TestName name = new TestName();

    private File logDir;

    private ZooKeeperTestEnvironment zkCluster;

    private LocalShuffleCluster shuffleCluster;

    @Before
    public void setup() throws Exception {
        zkCluster = new ZooKeeperTestEnvironment(1);

        logDir =
                new File(
                        System.getProperty("buildDirectory")
                                + "/"
                                + getClass().getSimpleName()
                                + "-"
                                + name.getMethodName());
        if (logDir.exists()) {
            FileUtils.deleteDirectory(logDir);
        }

        Configuration configuration = new Configuration();
        configuration.setDuration(
                HeartbeatOptions.HEARTBEAT_WORKER_INTERVAL, Duration.ofMillis(2000L));
        configuration.setDuration(
                HeartbeatOptions.HEARTBEAT_WORKER_TIMEOUT, Duration.ofMillis(10000L));
        configuration.setDuration(
                HeartbeatOptions.HEARTBEAT_JOB_INTERVAL, Duration.ofMillis(5000L));
        configuration.setDuration(
                HeartbeatOptions.HEARTBEAT_JOB_TIMEOUT, Duration.ofMillis(30000L));
        shuffleCluster =
                new LocalShuffleCluster(
                        logDir.getAbsolutePath(),
                        2,
                        zkCluster.getConnect(),
                        temporaryFolder.newFolder().toPath(),
                        configuration);
        shuffleCluster.start();

        LOG.info("========== Test started ==========");
    }

    @After
    public void teardown() throws Exception {
        LOG.info("========== Test end ==========");

        try {
            shuffleCluster.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to stop shuffle cluster", e);
        }

        try {
            zkCluster.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to stop zk cluster", e);
        }
    }

    @Test
    public void testRevokeAndGrantLeadership() throws Exception {
        JobID jobId = new JobID();

        org.apache.flink.configuration.Configuration flinkConfiguration =
                new org.apache.flink.configuration.Configuration();
        flinkConfiguration.set(WebOptions.TIMEOUT, 15000L);
        flinkConfiguration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());

        FlinkLocalClusterResource resource = new FlinkLocalClusterResource(flinkConfiguration);
        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        try {
            FlinkLocalCluster flinkCluster = resource.getFlinkLocalCluster();
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            job.planOperation(
                    JobForShuffleTesting.STAGE0_NAME,
                    0,
                    0,
                    JobForShuffleTesting.TaskStat.RUNNING,
                    (processIdAndStat) -> {
                        CuratorFramework zkClient =
                                ZooKeeperTestUtils.createZKClientForRemoteShuffle(
                                        ZooKeeperTestUtils.createZooKeeperHAConfig(
                                                zkCluster.getConnect()));
                        // To cause ShuffleManager to lost its leadership.
                        zkClient.delete()
                                .forPath(
                                        ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.defaultValue()
                                                + ZooKeeperHaServices
                                                        .SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH);

                        // One shuffle worker down after 5s
                        Thread.sleep(5000);
                        shuffleCluster.killShuffleWorker(0);

                        // Wait till it heartbeat timeout
                        Thread.sleep(10000);
                    });

            job.run();
            checkFlinkResourceReleased(flinkCluster);
            checkShuffleResourceReleased();
        } catch (Throwable throwable) {
            resource.getFlinkLocalCluster().printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError("Test failure.", throwable);
        } finally {
            scheduledExecutor.shutdownNow();
            resource.close();
        }
    }

    private void checkShuffleResourceReleased() throws Exception {
        shuffleCluster.checkStorageResourceReleased();
        shuffleCluster.checkNetworkReleased();
        shuffleCluster.checkBuffersReleased();
    }

    private void checkFlinkResourceReleased(FlinkLocalCluster flinkLocalCluster) throws Exception {
        flinkLocalCluster.checkResourceReleased();
    }

    private class FlinkLocalClusterResource implements AutoCloseable {

        private final FlinkLocalCluster flinkLocalCluster;

        public FlinkLocalClusterResource(
                org.apache.flink.configuration.Configuration flinkConfiguration) throws Exception {
            flinkLocalCluster =
                    new FlinkLocalCluster(
                            logDir.getAbsolutePath(),
                            JobForShuffleTesting.PARALLELISM,
                            temporaryFolder,
                            zkCluster.getConnect(),
                            flinkConfiguration);
            flinkLocalCluster.start();
        }

        public FlinkLocalCluster getFlinkLocalCluster() {
            return flinkLocalCluster;
        }

        @Override
        public void close() throws Exception {
            flinkLocalCluster.shutdown();
        }
    }
}
