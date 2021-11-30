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
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.JobDataPartitionDistribution;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerRegistration;
import com.alibaba.flink.shuffle.core.config.HeartbeatOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.e2e.flinkcluster.FlinkLocalCluster;
import com.alibaba.flink.shuffle.e2e.shufflecluster.LocalShuffleCluster;
import com.alibaba.flink.shuffle.plugin.utils.IdMappingUtils;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.types.IntValue;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for scenario of ShuffleWorker lost. */
public class ShuffleWorkerLostE2ETest extends AbstractInstableE2ETest {

    @Test
    public void testShuffleWorkerLostOnWrite() throws Exception {
        JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
        job.setExecutionConfigModifier(
                conf -> conf.setRestartStrategy(fixedDelayRestart(5, Time.seconds(15))));

        job.planOperation(
                JobForShuffleTesting.STAGE0_NAME,
                0,
                0,
                JobForShuffleTesting.TaskStat.RUNNING,
                processIdAndStat -> {
                    int workerIndex = findWritingWorkerIndex(processIdAndStat);
                    shuffleCluster.killShuffleWorker(workerIndex);
                });

        try {
            job.run();
            checkFlinkResourceReleased();
            checkShuffleResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            ExceptionUtils.rethrowAsRuntimeException(t);
        }
    }

    @Test
    public void testShuffleWorkerGetLostOnRead() throws Exception {
        JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
        job.setExecutionConfigModifier(
                conf -> conf.setRestartStrategy(fixedDelayRestart(15, Time.seconds(15))));

        IntValue workerIndexToKill = new IntValue(-1);
        job.planOperation(
                JobForShuffleTesting.STAGE0_NAME,
                0,
                0,
                JobForShuffleTesting.TaskStat.RUNNING,
                processIdAndStat -> {
                    if (processIdAndStat.getStat() == JobForShuffleTesting.TaskStat.RUNNING) {
                        workerIndexToKill.setValue(findWritingWorkerIndex(processIdAndStat));
                    }
                });

        job.planOperation(
                JobForShuffleTesting.STAGE1_NAME,
                0,
                0,
                JobForShuffleTesting.TaskStat.RUNNING,
                processIdAndStat -> {
                    if (processIdAndStat.getStat() == JobForShuffleTesting.TaskStat.RUNNING) {
                        assertTrue(
                                "The worker index written is not recorded",
                                workerIndexToKill.getValue() >= 0);
                        shuffleCluster.killShuffleWorker(workerIndexToKill.getValue());
                    }
                });

        try {
            job.run();
            checkFlinkResourceReleased();
            checkShuffleResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            ExceptionUtils.rethrowAsRuntimeException(t);
        }
    }

    @Test
    public void testShuffleWorkerRecoveredAfterLostOnRead() throws Exception {
        JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
        job.setExecutionConfigModifier(
                conf -> conf.setRestartStrategy(fixedDelayRestart(10, Time.seconds(15))));

        ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

        IntValue workerIndexToKill = new IntValue(-1);
        job.planOperation(
                JobForShuffleTesting.STAGE0_NAME,
                0,
                0,
                JobForShuffleTesting.TaskStat.RUNNING,
                processIdAndStat -> {
                    if (processIdAndStat.getStat() == JobForShuffleTesting.TaskStat.RUNNING) {
                        workerIndexToKill.setValue(findWritingWorkerIndex(processIdAndStat));
                    }
                });

        job.planOperation(
                JobForShuffleTesting.STAGE1_NAME,
                0,
                0,
                JobForShuffleTesting.TaskStat.RUNNING,
                processIdAndStat -> {
                    if (processIdAndStat.getStat() == JobForShuffleTesting.TaskStat.RUNNING) {
                        assertTrue(
                                "The worker index written is not recorded",
                                workerIndexToKill.getValue() >= 0);
                        shuffleCluster.killShuffleWorker(workerIndexToKill.getValue());

                        scheduledExecutor.schedule(
                                () -> {
                                    try {
                                        shuffleCluster.recoverShuffleWorker(
                                                workerIndexToKill.getValue());
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                },
                                10,
                                TimeUnit.SECONDS);
                    }
                });

        try {
            job.run();

            // check that stage0 no restart, check that stage1 restart once.
            for (int i = 0; i < job.getParallelism(); ++i) {
                job.checkAttemptsNum(JobForShuffleTesting.STAGE0_NAME, i, 1);
            }
            checkFlinkResourceReleased();
            checkShuffleResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            ExceptionUtils.rethrowAsRuntimeException(t);
        }
    }

    private Integer findWritingWorkerIndex(
            JobForShuffleTesting.ProcessIDAndTaskStat processIdAndStat) throws Exception {
        List<JobID> jobs = shuffleCluster.shuffleManagerClient.listJobs(false).get();

        assertEquals(1, jobs.size());

        JobDataPartitionDistribution distribution =
                shuffleCluster
                        .shuffleManagerClient
                        .getJobDataPartitionDistribution(jobs.get(0))
                        .get();

        MapPartitionID mapPartitionId =
                IdMappingUtils.fromFlinkResultPartitionID(processIdAndStat.resultPartitionID);
        Optional<Map.Entry<DataPartitionCoordinate, ShuffleWorkerRegistration>> foundWorker =
                distribution.getDataPartitionDistribution().entrySet().stream()
                        .filter(e -> e.getKey().getDataPartitionId().equals(mapPartitionId))
                        .findFirst();
        assertTrue(
                "The produced data partition is not found in "
                        + distribution.getDataPartitionDistribution(),
                foundWorker.isPresent());

        Optional<Integer> workerIndex =
                shuffleCluster.findShuffleWorker(foundWorker.get().getValue().getProcessID());
        assertTrue(workerIndex.isPresent());

        return workerIndex.get();
    }

    private void checkShuffleResourceReleased() throws Exception {
        shuffleCluster.checkStorageResourceReleased();
        shuffleCluster.checkNetworkReleased();
        shuffleCluster.checkBuffersReleased();
    }

    private void checkFlinkResourceReleased() throws Exception {
        flinkCluster.checkResourceReleased();
    }

    @Override
    protected LocalShuffleCluster createLocalShuffleCluster(
            String logPath, String zkConnect, Path dataPath) {
        Configuration conf = new Configuration();
        conf.setDuration(HeartbeatOptions.HEARTBEAT_WORKER_INTERVAL, Duration.ofSeconds(5));
        conf.setDuration(HeartbeatOptions.HEARTBEAT_WORKER_TIMEOUT, Duration.ofSeconds(20));
        return new LocalShuffleCluster(logPath, 2, zkConnect, dataPath, conf);
    }

    @Override
    protected FlinkLocalCluster createFlinkCluster(
            String logPath, TemporaryFolder tmpFolder, String zkConnect) throws Exception {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        conf.setString(WorkerOptions.MAX_WORKER_RECOVER_TIME.key(), "80s");
        return new FlinkLocalCluster(logPath, 2, tmpFolder, zkConnect, conf);
    }
}
