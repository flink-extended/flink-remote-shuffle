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
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetricKeys;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.config.HeartbeatOptions;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.e2e.flinkcluster.FlinkLocalCluster;
import com.alibaba.flink.shuffle.e2e.shufflecluster.LocalShuffleCluster;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestEnvironment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.configuration.WebOptions;

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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.fail;

/** Tests for scenario of ShuffleManager lost. */
public class ShuffleManagerLostE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleWorkerLostE2ETest.class);

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public TestName name = new TestName();

    private File logDir;

    private ZooKeeperTestEnvironment zkCluster;

    private LocalShuffleCluster localShuffleCluster;

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
                HeartbeatOptions.HEARTBEAT_WORKER_INTERVAL, Duration.ofSeconds(5));
        configuration.setDuration(
                HeartbeatOptions.HEARTBEAT_WORKER_TIMEOUT, Duration.ofSeconds(20));
        configuration.setDuration(HeartbeatOptions.HEARTBEAT_JOB_INTERVAL, Duration.ofSeconds(5));
        configuration.setDuration(HeartbeatOptions.HEARTBEAT_JOB_TIMEOUT, Duration.ofSeconds(30));
        localShuffleCluster =
                new LocalShuffleCluster(
                        logDir.getAbsolutePath(),
                        2,
                        zkCluster.getConnect(),
                        temporaryFolder.newFolder().toPath(),
                        configuration);
        localShuffleCluster.start();

        LOG.info("========== Test started ==========");
    }

    @After
    public void teardown() throws Exception {
        LOG.info("========== Test end ==========");

        try {
            localShuffleCluster.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to stop the local shuffle cluster", e);
        }

        try {
            zkCluster.shutdown();
        } catch (Exception e) {
            LOG.info("Failed to stop zk cluster", e);
        }
    }

    @Test
    public void testShuffleManagerRecoveredAfterLostOnWrite() throws Exception {
        JobID jobId = new JobID();

        org.apache.flink.configuration.Configuration flinkConfiguration =
                new org.apache.flink.configuration.Configuration();
        flinkConfiguration.set(WebOptions.TIMEOUT, 15000L);
        flinkConfiguration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());

        try (FlinkLocalClusterResource resource =
                new FlinkLocalClusterResource(flinkConfiguration)) {
            JobForShuffleTesting job = new JobForShuffleTesting(resource.getFlinkLocalCluster());
            ScheduledExecutorService scheduledExecutor =
                    Executors.newSingleThreadScheduledExecutor();

            try {
                job.planOperation(
                        JobForShuffleTesting.STAGE0_NAME,
                        0,
                        0,
                        JobForShuffleTesting.TaskStat.RUNNING,
                        (processIdAndStat) -> {
                            localShuffleCluster.killShuffleManager();

                            scheduledExecutor.schedule(
                                    () -> {
                                        try {
                                            localShuffleCluster.recoverShuffleManager();
                                        } catch (Exception e) {
                                            LOG.error("Failed to recover shuffle manager", e);
                                        }
                                    },
                                    10,
                                    TimeUnit.SECONDS);
                        });

                try {
                    job.run();
                    checkFlinkResourceReleased(resource.getFlinkLocalCluster());
                    checkShuffleResourceReleased();
                } catch (Exception e) {
                    // Ignored
                }
            } finally {
                scheduledExecutor.shutdownNow();
            }
        }
    }

    /**
     * Tests the shuffle manager get restarted and during this period the job just do not have
     * requests. If so, the job won't failover due to fatal error. After the shuffle manager
     * restarted, it would not notify client to remove partitions for some time since it would have
     * some time to synchronize with the shuffle workers. Therefore, the job will continue to run
     * until success.
     *
     * @throws Exception
     */
    @Test
    public void testShuffleManagerRecoveredAfterLostWithoutJobFailover() throws Exception {
        org.apache.flink.configuration.Configuration flinkConfiguration =
                new org.apache.flink.configuration.Configuration();

        ExecutorService scheduledExecutor =
                Executors.newFixedThreadPool(JobForShuffleTesting.PARALLELISM);
        try (FlinkLocalClusterResource resource =
                new FlinkLocalClusterResource(flinkConfiguration)) {
            JobForShuffleTesting job = new JobForShuffleTesting(resource.getFlinkLocalCluster());

            AtomicInteger stage1LatchStarted = new AtomicInteger(0);
            CountDownLatch shuffleManagerRestartedLatch = new CountDownLatch(1);

            for (int i = 0; i < JobForShuffleTesting.PARALLELISM; ++i) {
                job.planAsyncOperation(
                        JobForShuffleTesting.STAGE1_NAME,
                        i,
                        0,
                        JobForShuffleTesting.TaskStat.RUNNING,
                        (processIdAndStat, nodeCache) -> {
                            CompletableFuture<Void> resultFuture = new CompletableFuture<>();
                            if (processIdAndStat.getStat()
                                    == JobForShuffleTesting.TaskStat.RUNNING) {
                                scheduledExecutor.execute(
                                        () -> {
                                            try {
                                                int count = stage1LatchStarted.incrementAndGet();
                                                LOG.info("Increase the count to {}", count);
                                                if (count == JobForShuffleTesting.PARALLELISM) {
                                                    LOG.info(
                                                            "Increase the count to {} and restart the shuffle manager",
                                                            count);

                                                    localShuffleCluster.killShuffleManager();
                                                    Thread.sleep(5000);

                                                    localShuffleCluster.recoverShuffleManager();
                                                    Thread.sleep(5000);

                                                    shuffleManagerRestartedLatch.countDown();
                                                }

                                                shuffleManagerRestartedLatch.await();
                                                resultFuture.complete(null);
                                            } catch (Exception e) {
                                                resultFuture.completeExceptionally(e);
                                            }
                                        });
                            } else {
                                resultFuture.complete(null);
                            }
                            return resultFuture;
                        });
            }

            // The job should finished without failover
            job.run();
        } finally {
            scheduledExecutor.shutdownNow();
        }
    }

    @Test
    public void testPartitionCleanupOnJobFinished() throws Exception {
        org.apache.flink.configuration.Configuration flinkConfiguration =
                new org.apache.flink.configuration.Configuration();
        try (FlinkLocalClusterResource resource =
                new FlinkLocalClusterResource(flinkConfiguration)) {
            JobForShuffleTesting job = new JobForShuffleTesting(resource.getFlinkLocalCluster());
            job.run();
        }

        LOG.info("Job finished, now check the cleanup");
        // Now let's keep check till all the jobs are cleanup
        waitTilAllResultPartitionsReleased(80);
    }

    @Test
    public void testPartitionCleanupOnJobKilled() throws Exception {
        org.apache.flink.configuration.Configuration flinkConfiguration =
                new org.apache.flink.configuration.Configuration();
        flinkConfiguration.setString(HeartbeatOptions.HEARTBEAT_JOB_INTERVAL.key(), "5s");
        flinkConfiguration.setString(HeartbeatOptions.HEARTBEAT_JOB_TIMEOUT.key(), "30s");
        FlinkLocalClusterResource resource = new FlinkLocalClusterResource(flinkConfiguration);
        JobForShuffleTesting job = new JobForShuffleTesting(resource.getFlinkLocalCluster());

        job.planOperation(
                JobForShuffleTesting.STAGE2_NAME,
                0,
                0,
                JobForShuffleTesting.TaskStat.OPENED,
                (taskStat) -> resource.getFlinkLocalCluster().shutdown());

        try {
            job.run();
            fail("The job should be failed deu to get killed");
        } catch (Exception e) {
            // ignored
        }

        LOG.info("Job killed, now check the cleanup");
        // Now let's keep check till all the jobs are cleanup
        waitTilAllResultPartitionsReleased(80);
    }

    private void waitTilAllResultPartitionsReleased(int timeoutInSeconds)
            throws ExecutionException, InterruptedException {
        // Now let's keep check till all the jobs are cleanup
        Deadline deadline = Deadline.fromNow(Duration.ofSeconds(timeoutInSeconds));
        while (true) {
            Thread.sleep(5000);

            List<com.alibaba.flink.shuffle.core.ids.JobID> jobIds =
                    localShuffleCluster.shuffleManagerClient.listJobs(false).get();
            LOG.info("Check jobs and get {}", jobIds);
            if (jobIds.size() > 0) {
                if (!deadline.hasTimeLeft()) {
                    fail("There is still jobs left: " + jobIds);
                } else {
                    continue;
                }
            }

            Map<InstanceID, ShuffleWorkerMetrics> shuffleWorkerMetricMap =
                    localShuffleCluster.shuffleManagerClient.getShuffleWorkerMetrics().get();
            LOG.info("Check workers and get {}", shuffleWorkerMetricMap);
            boolean hasRemainingPartitions =
                    shuffleWorkerMetricMap.entrySet().stream()
                            .anyMatch(
                                    entry ->
                                            entry.getValue()
                                                            .getIntegerMetric(
                                                                    ShuffleWorkerMetricKeys
                                                                            .DATA_PARTITION_NUMBERS_KEY)
                                                    > 0);
            if (hasRemainingPartitions) {
                if (!deadline.hasTimeLeft()) {
                    fail("There is still partitions left: " + shuffleWorkerMetricMap);
                } else {
                    continue;
                }
            }

            // Here it means all the result partitions are released;
            break;
        }
    }

    private void checkShuffleResourceReleased() throws Exception {
        localShuffleCluster.checkStorageResourceReleased();
        localShuffleCluster.checkNetworkReleased();
        localShuffleCluster.checkBuffersReleased();
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
