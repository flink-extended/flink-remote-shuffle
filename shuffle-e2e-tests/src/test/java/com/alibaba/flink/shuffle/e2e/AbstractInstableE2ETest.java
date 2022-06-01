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
import com.alibaba.flink.shuffle.e2e.flinkcluster.FlinkLocalCluster;
import com.alibaba.flink.shuffle.e2e.shufflecluster.LocalShuffleCluster;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestEnvironment;

import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.nio.file.Path;

/** Base class for instable tests. */
public class AbstractInstableE2ETest {

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Rule public TestName name = new TestName();

    protected ZooKeeperTestEnvironment zkEnv;

    protected CuratorFramework zkClient;

    protected LocalShuffleCluster shuffleCluster;

    protected FlinkLocalCluster flinkCluster;

    @Before
    public void setup() throws Exception {
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

        zkEnv = new ZooKeeperTestEnvironment();
        String zkConnect = zkEnv.getConnect();
        String logPath = logDir.getAbsolutePath();
        shuffleCluster =
                createLocalShuffleCluster(logPath, zkConnect, tmpFolder.newFolder().toPath());
        shuffleCluster.start();

        Exception exception = null;
        for (int i = 0; i < 3; ++i) {
            try {
                flinkCluster = createFlinkCluster(logDir.getAbsolutePath(), tmpFolder, zkConnect);
                flinkCluster.start();
                break;
            } catch (Exception throwable) {
                exception = exception == null ? throwable : exception;
                flinkCluster.shutdown();
                flinkCluster = null;
            }
        }

        if (flinkCluster == null) {
            throw new Exception(exception);
        }
        zkClient = flinkCluster.getZKClient();
    }

    @After
    public void cleanup() throws Exception {
        flinkCluster.shutdown();

        shuffleCluster.shutdown();

        zkEnv.shutdown();
    }

    protected LocalShuffleCluster createLocalShuffleCluster(
            String logPath, String zkConnect, Path dataPath) {
        return new LocalShuffleCluster(logPath, 2, zkConnect, dataPath, new Configuration());
    }

    protected FlinkLocalCluster createFlinkCluster(
            String logPath, TemporaryFolder tmpFolder, String zkConnect) throws Exception {
        return new FlinkLocalCluster(
                logPath,
                2,
                tmpFolder,
                zkConnect,
                new org.apache.flink.configuration.Configuration());
    }
}
