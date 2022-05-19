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

package com.alibaba.flink.shuffle.coordinator.worker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.RestOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** Tests the behavior of the {@link ShuffleWorkerRunner}. */
public class ShuffleWorkerRunnerTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Rule public final Timeout timeout = Timeout.seconds(30);

    private ShuffleWorkerRunner shuffleWorkerRunner;

    @After
    public void after() throws Exception {
        System.setSecurityManager(null);
        if (shuffleWorkerRunner != null) {
            shuffleWorkerRunner.close();
        }
    }

    @Test
    public void testShouldShutdownOnFatalError() throws Exception {
        Configuration configuration = createConfiguration();
        // very high timeout, to ensure that we don't fail because of registration timeouts
        configuration.setDuration(ClusterOptions.REGISTRATION_TIMEOUT, Duration.ofHours(42));
        shuffleWorkerRunner = createShuffleWorkerRunner(configuration);

        shuffleWorkerRunner.onFatalError(new RuntimeException("Test Exception"));

        assertEquals(
                ShuffleWorkerRunner.Result.FAILURE,
                shuffleWorkerRunner.getTerminationFuture().get());
    }

    @Test
    public void testShouldShutdownIfRegistrationWithShuffleManagerFails() throws Exception {
        Configuration configuration = createConfiguration();
        configuration.setDuration(ClusterOptions.REGISTRATION_TIMEOUT, Duration.ofMillis(10));
        shuffleWorkerRunner = createShuffleWorkerRunner(configuration);

        assertEquals(
                ShuffleWorkerRunner.Result.FAILURE,
                shuffleWorkerRunner.getTerminationFuture().get());
    }

    private static Configuration createConfiguration() throws IOException {
        Configuration configuration = new Configuration();
        File baseDir = TEMP_FOLDER.newFolder();
        String basePath = baseDir.getAbsolutePath() + "/";

        configuration.setString(ManagerOptions.RPC_ADDRESS, "localhost");
        configuration.setString(WorkerOptions.HOST, "localhost");
        configuration.setString(StorageOptions.STORAGE_LOCAL_DATA_DIRS, basePath);

        // choose random worker port
        Random random = new Random(System.currentTimeMillis());
        int nextPort = random.nextInt(30000) + 20000;
        configuration.setInteger(TransferOptions.SERVER_DATA_PORT, nextPort);

        nextPort = random.nextInt(30000) + 20000;
        configuration.setInteger(RestOptions.REST_WORKER_BIND_PORT, nextPort);

        return configuration;
    }

    private static ShuffleWorkerRunner createShuffleWorkerRunner(Configuration configuration)
            throws Exception {
        configuration.setMemorySize(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_READING, MemoryOptions.MIN_VALID_MEMORY_SIZE);
        configuration.setMemorySize(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING, MemoryOptions.MIN_VALID_MEMORY_SIZE);
        ShuffleWorkerRunner shuffleWorkerRunner = new ShuffleWorkerRunner(configuration);
        shuffleWorkerRunner.start();
        return shuffleWorkerRunner;
    }
}
