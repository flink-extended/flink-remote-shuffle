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

package com.alibaba.flink.shuffle.yarn;

import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner;
import com.alibaba.flink.shuffle.yarn.entry.manager.AppClient;
import com.alibaba.flink.shuffle.yarn.entry.worker.YarnShuffleWorkerEntrypoint;
import com.alibaba.flink.shuffle.yarn.utils.YarnConstants;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator4.org.apache.curator.retry.RetryNTimes;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.Service;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions.HA_MODE;
import static com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM;
import static com.alibaba.flink.shuffle.core.config.ManagerOptions.RPC_BIND_PORT;
import static com.alibaba.flink.shuffle.core.config.ManagerOptions.RPC_PORT;
import static com.alibaba.flink.shuffle.core.config.MemoryOptions.MEMORY_SIZE_FOR_DATA_READING;
import static com.alibaba.flink.shuffle.core.config.MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING;
import static com.alibaba.flink.shuffle.core.config.MemoryOptions.MIN_VALID_MEMORY_SIZE;
import static com.alibaba.flink.shuffle.core.config.StorageOptions.STORAGE_LOCAL_DATA_DIRS;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_TMP_PATH_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** IT case with ShuffleManager and ShuffleWorkers deployed on Yarn framework. */
public class RemoteShuffleOnYarnTestCluster extends YarnTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleOnYarnTestCluster.class);

    private static final TemporaryFolder tmp = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws Exception {
        client = startCuratorFramework();
        client.start();

        setupConfigurations();

        startYARNWithRetries(YARN_CONFIGURATION, false);

        deployShuffleManager();

        checkServiceRunning();
    }

    private static void setupConfigurations() throws Exception {
        YARN_CONFIGURATION.set(TEST_CLUSTER_NAME_KEY, "remote-shuffle-on-yarn-tests");

        // HA service configurations
        YARN_CONFIGURATION.set(HA_MODE.key(), "ZOOKEEPER");
        YARN_CONFIGURATION.set(HA_ZOOKEEPER_QUORUM.key(), zookeeperTestServer.getConnectString());

        // Shuffle worker configurations
        YARN_CONFIGURATION.set(
                "yarn.nodemanager.aux-services", YarnConstants.WORKER_AUXILIARY_SERVICE_NAME);
        YARN_CONFIGURATION.set(
                "yarn.nodemanager.aux-services."
                        + YarnConstants.WORKER_AUXILIARY_SERVICE_NAME
                        + ".class",
                YarnShuffleWorkerEntrypoint.class.getCanonicalName());
        YARN_CONFIGURATION.set(STORAGE_LOCAL_DATA_DIRS.key(), "[HDD]" + getStorageDataDirs());
        YARN_CONFIGURATION.set(
                MEMORY_SIZE_FOR_DATA_WRITING.key(), MIN_VALID_MEMORY_SIZE.toString());
        YARN_CONFIGURATION.set(
                MEMORY_SIZE_FOR_DATA_READING.key(), MIN_VALID_MEMORY_SIZE.toString());
    }

    /** Simulate the submission workflow to start the Shuffle Manager. */
    private static void deployShuffleManager() throws Exception {
        File shuffleHomeDir = new File(findShuffleLocalHomeDir());
        assertTrue(shuffleHomeDir.exists());
        String mockArgs =
                "-D "
                        + YarnConstants.MANAGER_HOME_DIR
                        + "="
                        + findShuffleLocalHomeDir()
                        + " -D "
                        + YarnConstants.MANAGER_AM_MEMORY_SIZE_KEY
                        + "=128 -D "
                        + YarnConstants.MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY
                        + "=128 -D "
                        + YarnConstants.MANAGER_APP_QUEUE_NAME_KEY
                        + "=root.default -D "
                        + RPC_PORT.key()
                        + "=23123 -D "
                        + RPC_BIND_PORT.key()
                        + "=23123 -D "
                        + HA_MODE.key()
                        + "=ZOOKEEPER -D"
                        + HA_ZOOKEEPER_QUORUM.key()
                        + "="
                        + zookeeperTestServer.getConnectString();
        AppClient client = new AppClient(mockArgs.split("\\s+"), YARN_CONFIGURATION);
        assertTrue(client.submitApplication());
    }

    private static void checkServiceRunning() throws IOException {
        assertSame(yarnCluster.getServiceState(), Service.STATE.STARTED);
        checkFileInHdfsExists();
        checkShuffleWorkerRunning();
        LOG.info("All services are good");
    }

    private static void checkShuffleWorkerRunning() {
        // Check Shuffle Worker is running as a auxiliary service of Node Manager
        Thread workerRunnerThread =
                new Thread(
                        () -> {
                            try {
                                ShuffleWorkerRunner shuffleWorkerRunner =
                                        YarnShuffleWorkerEntrypoint.getShuffleWorkerRunner();
                                assertNotNull(shuffleWorkerRunner);
                                assertEquals(
                                        shuffleWorkerRunner.getTerminationFuture().get(),
                                        ShuffleWorkerRunner.Result.SUCCESS);
                                LOG.info(
                                        "Shuffle Worker runner status: "
                                                + shuffleWorkerRunner.getTerminationFuture().get());
                            } catch (Exception e) {
                                LOG.error("Shuffle Worker encountered an exception, ", e);
                                Assert.fail(e.getMessage());
                            }
                        });
        workerRunnerThread.start();
    }

    private static File getHomeDir() {
        File homeDir = null;
        try {
            tmp.create();
            homeDir = tmp.newFolder();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        System.setProperty("user.home", homeDir.getAbsolutePath());
        return homeDir;
    }

    private static String getStorageDataDirs() {
        File storageDir = new File(getHomeDir().getAbsolutePath(), "dataStorage");
        if (!storageDir.exists()) {
            assertTrue(storageDir.mkdirs());
            assertTrue(storageDir.exists());
        }
        return storageDir.getAbsolutePath();
    }

    private static CuratorFramework startCuratorFramework() throws Exception {
        return CuratorFrameworkFactory.builder()
                .connectString(zookeeperTestServer.getConnectString())
                .retryPolicy(new RetryNTimes(50, 100))
                .build();
    }

    private static void checkFileInHdfsExists() throws IOException {
        FileSystem fs = FileSystem.get(YARN_CONFIGURATION);
        Path hdfsPath = new Path(fs.getHomeDirectory(), MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME + "/");
        FileStatus[] appDirs = fs.listStatus(hdfsPath);
        assertEquals(1, appDirs.length);
        Path shuffleManagerWorkDir =
                new Path(appDirs[0].getPath(), MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME);
        FileStatus[] workDir = fs.listStatus(shuffleManagerWorkDir);
        assertTrue(workDir.length > 0);
        FileStatus[] tmpDir =
                fs.listStatus(new Path(shuffleManagerWorkDir, MANAGER_AM_TMP_PATH_NAME));
        assertTrue(tmpDir.length > 0);
        StringBuilder fileNames = new StringBuilder();
        Arrays.stream(tmpDir).forEach(curFile -> fileNames.append(",").append(curFile.getPath()));
        assertTrue(
                fileNames.toString().contains("shuffle-dist")
                        && fileNames.toString().contains("log4j")
                        && fileNames.toString().contains(MANAGER_AM_TMP_PATH_NAME)
                        && fileNames.toString().contains(MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME));
    }

    private static String findShuffleLocalHomeDir() throws IOException {
        File parentDir =
                findSpecificDirectory("../shuffle-dist/target/", "flink-remote-shuffle-", "-bin");
        File found =
                findSpecificDirectory(parentDir.getAbsolutePath(), "flink-remote-shuffle-", "");
        if (found == null) {
            throw new IOException("Can't find lib in " + parentDir.getAbsolutePath());
        }
        return found.getAbsolutePath();
    }

    private static File findSpecificDirectory(String startAt, String prefix, String suffix) {
        File[] subFiles = (new File(startAt)).listFiles();
        assertNotNull(subFiles);
        File found = null;
        for (File curFile : subFiles) {
            if (curFile.isDirectory()) {
                if (curFile.getName().startsWith(prefix) && curFile.getName().endsWith(suffix)) {
                    found = curFile;
                    break;
                }
                found = findSpecificDirectory(curFile.getAbsolutePath(), prefix, suffix);
            }
        }
        return found;
    }
}
