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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.HeartbeatOptions;
import com.alibaba.flink.shuffle.e2e.shufflecluster.LocalShuffleCluster;

import org.junit.Test;

import java.nio.file.Path;
import java.time.Duration;

import static org.junit.Assert.assertEquals;

/** Tests for instable Zookeeper. */
public class InstableZookeeperE2ETest extends AbstractInstableE2ETest {

    @Test(timeout = 180000)
    public void testZookeeperRestart() throws Exception {
        assertNumWorkers(2);

        zkEnv.restart();
        assertNumWorkers(2);

        shuffleCluster.killShuffleWorker(0);
        assertNumWorkers(1);
        for (int i = 0; i < 100; ++i) {
            assertEquals(1, shuffleCluster.getNumRegisteredWorkers());
            Thread.sleep(100);
        }

        shuffleCluster.recoverShuffleWorker(0);
        assertNumWorkers(2);
        for (int i = 0; i < 100; ++i) {
            assertEquals(2, shuffleCluster.getNumRegisteredWorkers());
            Thread.sleep(100);
        }
    }

    private void assertNumWorkers(int expected) throws Exception {
        while (true) {
            try {
                int numWorkers = shuffleCluster.getNumRegisteredWorkers();
                if (numWorkers == expected) {
                    break;
                }
            } catch (Throwable ignored) {
                Thread.sleep(100);
            }
        }
    }

    @Override
    protected LocalShuffleCluster createLocalShuffleCluster(
            String logPath, String zkConnect, Path dataPath) {
        Configuration configuration = new Configuration();
        configuration.setDuration(
                HeartbeatOptions.HEARTBEAT_WORKER_INTERVAL, Duration.ofSeconds(1));
        configuration.setDuration(HeartbeatOptions.HEARTBEAT_WORKER_TIMEOUT, Duration.ofSeconds(5));
        return new LocalShuffleCluster(logPath, 2, zkConnect, dataPath, configuration);
    }
}
