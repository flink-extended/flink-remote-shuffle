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

package com.alibaba.flink.shuffle.e2e.shufflecluster;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetricKeys;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.e2e.utils.CommonTestUtils;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestEnvironment;

import org.apache.flink.api.common.time.Deadline;

import org.apache.commons.io.FileUtils;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link LocalShuffleCluster}. */
public class LocalShuffleClusterE2ETest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public TestName name = new TestName();

    private ZooKeeperTestEnvironment zkCluster;

    private LocalShuffleCluster cluster;

    @Before
    public void setup() throws IOException {
        File logDir =
                new File(
                        System.getProperty("buildDirectory")
                                + "/"
                                + getClass().getSimpleName()
                                + "-"
                                + name.getMethodName());
        if (logDir.exists()) {
            FileUtils.deleteDirectory(logDir);
        }

        zkCluster = new ZooKeeperTestEnvironment(1);
        cluster =
                new LocalShuffleCluster(
                        logDir.getAbsolutePath(),
                        2,
                        zkCluster.getConnect(),
                        temporaryFolder.newFolder().toPath(),
                        new Configuration());
    }

    @After
    public void cleanup() throws Exception {
        cluster.shutdown();
        zkCluster.shutdown();
    }

    @Test(timeout = 300000L)
    public void testKillShuffleWorker() throws Exception {

        cluster.start();
        MatcherAssert.assertThat(
                cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get(), is(2));

        cluster.killShuffleWorker(0);

        CommonTestUtils.waitUntilCondition(
                () -> cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get() == 1,
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout.");
    }

    @Test(timeout = 300000L)
    public void testKillShuffleWorkerForcibly() throws Exception {

        cluster.start();
        MatcherAssert.assertThat(
                cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get(), is(2));

        cluster.killShuffleWorkerForcibly(0);

        CommonTestUtils.waitUntilCondition(
                () -> cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get() == 1,
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout.");
    }

    @Test(timeout = 300000L)
    public void testRecoverShuffleWorker() throws Exception {
        cluster.start();
        MatcherAssert.assertThat(
                cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get(), is(2));

        cluster.killShuffleWorkerForcibly(0);
        CommonTestUtils.waitUntilCondition(
                () -> cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get() == 1,
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout.");
        assertThat(cluster.isShuffleWorkerAlive(0), is(false));

        // recover
        cluster.recoverShuffleWorker(0);
        CommonTestUtils.waitUntilCondition(
                () -> cluster.shuffleManagerClient.getNumberOfRegisteredWorkers().get() == 2,
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout.");
        assertThat(cluster.isShuffleWorkerAlive(0), is(true));
    }

    @Test(timeout = 300000L)
    public void testGetShuffleWorkerMetrics() throws Exception {
        cluster.start();
        Collection<ShuffleWorkerMetrics> shuffleWorkerMetrics =
                cluster.shuffleManagerClient.getShuffleWorkerMetrics().get().values();
        for (ShuffleWorkerMetrics metric : shuffleWorkerMetrics) {
            Configuration configuration = cluster.getConfig();
            MemorySize bufferSize = configuration.getMemorySize(MemoryOptions.MEMORY_BUFFER_SIZE);

            int actualR =
                    metric.getIntegerMetric(ShuffleWorkerMetricKeys.AVAILABLE_READING_BUFFERS_KEY);
            MemorySize rMemorySize =
                    configuration.getMemorySize(MemoryOptions.MEMORY_SIZE_FOR_DATA_READING);
            int expectR = (int) (rMemorySize.getBytes() / bufferSize.getBytes());
            assertThat(actualR, is(expectR));

            int actualW =
                    metric.getIntegerMetric(ShuffleWorkerMetricKeys.AVAILABLE_WRITING_BUFFERS_KEY);
            MemorySize wMemorySize =
                    configuration.getMemorySize(MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING);
            int expectW = (int) (wMemorySize.getBytes() / bufferSize.getBytes());
            assertThat(actualW, is(expectW));
        }
    }
}
