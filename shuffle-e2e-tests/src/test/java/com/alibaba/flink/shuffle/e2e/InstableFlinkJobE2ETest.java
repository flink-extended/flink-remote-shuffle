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

package com.alibaba.flink.shuffle.e2e;

import com.alibaba.flink.shuffle.common.functions.ConsumerWithException;
import com.alibaba.flink.shuffle.common.functions.RunnableWithException;

import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.runQuietly;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.STAGE0_NAME;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.STAGE1_NAME;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.STAGE2_NAME;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.TaskStat;

/** Tests for instable Flink job. */
public class InstableFlinkJobE2ETest extends AbstractInstableE2ETest {

    @Test
    public void testBasicRoutine() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            job.run();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testCancelJob() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            RunnableWithException procedure =
                    () -> {
                        ConsumerWithException runnable = ignore -> flinkCluster.cancelJobs();
                        job.planOperation(STAGE0_NAME, 0, 0, TaskStat.RUNNING, runnable);
                        job.run();
                    };
            runQuietly(procedure::run);
            job.checkNoResult();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testShuffleWriteTaskFailureAndRecovery() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            job.planFailingARunningTask(STAGE0_NAME, 0, 0);
            job.run();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testShuffleReadWriteTaskFailureAndRecovery() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            job.planFailingARunningTask(STAGE1_NAME, 0, 0);
            job.run();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testShuffleReadTaskFailureAndRecovery() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            job.planFailingARunningTask(STAGE2_NAME, 0, 0);
            job.run();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testShuffleWriteTaskIOFailure() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            ConsumerWithException r =
                    ignore -> {
                        for (int i = 0; i < shuffleCluster.shuffleWorkers.length; ++i) {
                            Stream<Path> paths =
                                    Files.list(
                                            new File(shuffleCluster.getDataDirForWorker(i))
                                                    .toPath());
                            for (Path p : paths.collect(Collectors.toList())) {
                                if (!p.toString().endsWith("_meta")) {
                                    Files.deleteIfExists(p);
                                }
                            }
                        }
                    };
            job.planOperation(STAGE0_NAME, 0, 0, TaskStat.RUNNING, r);
            job.run();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testShuffleReadWriteTaskIOFailure() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            ConsumerWithException r =
                    ignore -> {
                        for (int i = 0; i < shuffleCluster.shuffleWorkers.length; ++i) {
                            Stream<Path> paths =
                                    Files.list(
                                            new File(shuffleCluster.getDataDirForWorker(i))
                                                    .toPath());
                            for (Path p : paths.collect(Collectors.toList())) {
                                if (!p.toString().endsWith("_meta")
                                        && !p.toString().endsWith("partial")) {
                                    Files.deleteIfExists(p);
                                }
                            }
                        }
                    };
            job.planOperation(STAGE1_NAME, 0, 0, TaskStat.RUNNING, r);
            job.run();

            // Longest failover is as below:
            // 1. Failure when reading&writing, because all remote files removed;
            // 2. Retry but fetch failed due to PartitionException on (STAGE0_NAME, 0);
            // 3. Retry but fetch failed due to PartitionException on (STAGE0_NAME, 1);
            // 4. Retry and succeed;
            // job.checkTaskInfoOnZK(STAGE1_NAME, 0, 4, false);
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }

    @Test
    public void testShuffleReadTaskIOFailure() {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster);
            ConsumerWithException r =
                    ignore -> {
                        for (int i = 0; i < shuffleCluster.shuffleWorkers.length; ++i) {
                            Stream<Path> paths =
                                    Files.list(Paths.get(shuffleCluster.getDataDirForWorker(i)));
                            for (Path p : paths.collect(Collectors.toList())) {
                                if (!p.toString().endsWith("_meta")) {
                                    Files.deleteIfExists(p);
                                }
                            }
                        }
                    };
            job.planOperation(STAGE2_NAME, 0, 0, TaskStat.RUNNING, r);
            job.run();
            shuffleCluster.checkResourceReleased();
            flinkCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError(t);
        }
    }
}
