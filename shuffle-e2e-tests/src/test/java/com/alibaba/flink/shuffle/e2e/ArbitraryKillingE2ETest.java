/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.e2e.flinkcluster.FlinkLocalCluster;
import com.alibaba.flink.shuffle.e2e.shufflecluster.LocalShuffleCluster;

import org.apache.flink.configuration.TaskManagerOptions;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** Test arbitrary killing task manager, shuffle worker, shuffle manager. */
@RunWith(Parameterized.class)
public class ArbitraryKillingE2ETest extends AbstractInstableE2ETest {

    private static final int NUM_ROUNDS = 5;

    public ArbitraryKillingE2ETest(int ignore) {}

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Object[][] params = new Object[NUM_ROUNDS][1];
        params[0] = new Object[1];
        for (int i = 0; i < params.length; i++) {
            params[i][0] = i;
        }
        return Arrays.asList(params);
    }

    @Override
    protected FlinkLocalCluster createFlinkCluster(
            String logPath, TemporaryFolder tmpFolder, String zkConnect) throws Exception {
        org.apache.flink.configuration.Configuration conf =
                new org.apache.flink.configuration.Configuration();
        Random random = new Random();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 1 + random.nextInt(2));
        return new FlinkLocalCluster(logPath, 4, tmpFolder, zkConnect, conf);
    }

    @Override
    protected LocalShuffleCluster createLocalShuffleCluster(
            String logPath, String zkConnect, Path dataPath) {
        return new LocalShuffleCluster(logPath, 4, zkConnect, dataPath, new Configuration());
    }

    @Test
    public void test() {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<?> future = executor.submit(this::testRoutine);
        try {
            future.get(360, TimeUnit.SECONDS);
        } catch (Exception e) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError("Test failure.", e);
        } finally {
            executor.shutdown();
        }
    }

    private void testRoutine() {
        AtomicReference<StringBuilder> killingScript = new AtomicReference<>(new StringBuilder());
        AtomicReference<Throwable> cause = new AtomicReference<>(null);
        try {
            AtomicBoolean finished = new AtomicBoolean(false);
            JobForShuffleTesting job =
                    new JobForShuffleTesting(
                            flinkCluster, 4, JobForShuffleTesting.DataScale.NORMAL);
            startArbitraryKiller(finished, cause, killingScript);
            job.run();
            finished.set(true);
            if (cause.get() != null) {
                throw cause.get();
            }
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            throw new AssertionError("Killing script: " + killingScript.get().toString(), t);
        }
    }

    private void startArbitraryKiller(
            AtomicBoolean finished,
            AtomicReference<Throwable> cause,
            AtomicReference<StringBuilder> killingScript) {
        Thread t =
                new Thread(
                        () -> {
                            try {
                                Random random = new Random();
                                int killTiming = random.nextInt(10);
                                killingScript
                                        .get()
                                        .append("Sleep for ")
                                        .append(killTiming)
                                        .append(" seconds.\n");
                                Thread.sleep(killTiming * 1000);
                                if (finished.get()) {
                                    return;
                                }
                                int x = random.nextInt(3);
                                if (x == 0) {
                                    // kill a task manager
                                    int tmIdx = random.nextInt(4);
                                    killingScript.get().append("Kill task manager ").append(tmIdx);
                                    flinkCluster.killTaskManager(tmIdx);
                                } else if (x == 1) {
                                    // kill a shuffle worker
                                    int swIdx = random.nextInt(3);
                                    killingScript
                                            .get()
                                            .append("Kill shuffle worker ")
                                            .append(swIdx);
                                    shuffleCluster.killShuffleWorkerForcibly(swIdx);
                                } else {
                                    killingScript.get().append("Kill shuffle manager");
                                    // TODO kill a shuffle manager
                                }
                            } catch (Exception e) {
                                cause.set(e);
                            }
                        });
        t.setDaemon(true);
        t.start();
    }
}
