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

import com.alibaba.flink.shuffle.core.utils.TestLogger;
import com.alibaba.flink.shuffle.yarn.zk.ZookeeperTestServer;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * The cluster is re-used for all tests based on Yarn framework.
 *
 * <p>The main goal of this class is to start {@link MiniYARNCluster} and {@link MiniDFSCluster}.
 * Users can run the Yarn test case on the mini clusters.
 */
public abstract class YarnTestBase extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(YarnTestBase.class);

    private static final int YARN_CLUSTER_START_RETRY_TIMES = 15;

    private static final int YARN_CLUSTER_START_RETRY_INTERVAL_MS = 20000;

    protected static ZookeeperTestServer zookeeperTestServer = new ZookeeperTestServer();

    protected static CuratorFramework client;

    protected static final String TEST_CLUSTER_NAME_KEY =
            "flink-remote-shuffle-yarn-minicluster-name";

    protected static final int NUM_NODEMANAGERS = 1;

    // Temp directory for mini hdfs cluster
    @ClassRule public static TemporaryFolder tmpHDFS = new TemporaryFolder();

    protected static MiniYARNCluster yarnCluster = null;

    protected static MiniDFSCluster miniDFSCluster = null;

    protected static final YarnConfiguration YARN_CONFIGURATION;

    protected static File yarnSiteXML = null;

    protected static File hdfsSiteXML = null;

    static {
        try {
            tmpHDFS.create();
        } catch (Exception e) {
            LOG.error("Create temporary folder failed, ", e);
        }
        YARN_CONFIGURATION = new YarnConfiguration();
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB, 32);
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
                2048); // 2048 is the available memory anyways
        YARN_CONFIGURATION.setBoolean(
                YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME, true);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 2);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_MAX_COMPLETED_APPLICATIONS, 2);
        YARN_CONFIGURATION.setInt(YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES, 4);
        YARN_CONFIGURATION.setInt(YarnConfiguration.DEBUG_NM_DELETE_DELAY_SEC, 3600);
        YARN_CONFIGURATION.setBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED, false);
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.NM_VCORES, 666); // memory is overwritten in the MiniYARNCluster.
        // so we have to change the number of cores for testing.
        YARN_CONFIGURATION.setInt(
                YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
                20000); // 20 seconds expiry (to ensure we properly heartbeat with YARN).
        YARN_CONFIGURATION.setFloat(
                YarnConfiguration.NM_MAX_PER_DISK_UTILIZATION_PERCENTAGE, 99.0F);
    }

    @After
    public static void shutdown() {
        zookeeperTestServer.afterOperations();
        if (yarnCluster != null) {
            yarnCluster.stop();
        }
        if (miniDFSCluster != null) {
            miniDFSCluster.shutdown();
        }
    }

    public static void startYARNWithRetries(YarnConfiguration conf, boolean withDFS) {
        for (int i = 0; i < YARN_CLUSTER_START_RETRY_TIMES; i++) {
            LOG.info("Waiting for the mini yarn cluster, retrying " + i + " times");
            boolean started = startYARNWithConfig(conf, withDFS);
            if (started) {
                LOG.info("Started yarn mini cluster successfully");
                return;
            }
        }
        LOG.info("Failed to start yarn mini cluster");
        Assert.fail();
    }

    private static boolean startYARNWithConfig(YarnConfiguration conf, boolean withDFS) {
        long deadline = System.currentTimeMillis() + YARN_CLUSTER_START_RETRY_INTERVAL_MS;
        try {
            LOG.info("Starting up MiniYARNCluster");
            if (yarnCluster == null) {
                setupMiniYarnCluster(conf);
            }

            File targetTestClassesFolder = new File("target/test-classes");
            writeYarnSiteConfigXML(conf, targetTestClassesFolder);

            if (withDFS) {
                LOG.info("Starting up MiniDFSCluster");
                setupMiniDFSCluster(targetTestClassesFolder);
            }

            assertSame(Service.STATE.STARTED, yarnCluster.getServiceState());

            // wait for the nodeManagers to connect
            boolean needWait = true;
            boolean started = false;
            while (needWait) {
                started = yarnCluster.waitForNodeManagersToConnect(500);
                LOG.info("Waiting for node managers to connect");
                needWait = !started && (System.currentTimeMillis() < deadline);
            }

            if (!started) {
                yarnCluster.stop();
                yarnCluster = null;
            }
            return started;
        } catch (Exception ex) {
            LOG.error("setup failure", ex);
            Assert.fail();
        }

        assertTrue(yarnCluster.getResourceManager().toString().endsWith("STARTED"));
        return true;
    }

    private static void setupMiniYarnCluster(YarnConfiguration conf) {
        final String testName = conf.get(YarnTestBase.TEST_CLUSTER_NAME_KEY);
        yarnCluster =
                new MiniYARNCluster(
                        testName == null ? "YarnTest_" + UUID.randomUUID() : testName,
                        NUM_NODEMANAGERS,
                        1,
                        1);

        yarnCluster.init(conf);
        yarnCluster.start();
    }

    private static void setupMiniDFSCluster(File targetTestClassesFolder) throws Exception {
        if (miniDFSCluster == null) {
            Configuration hdfsConfiguration = new Configuration();
            hdfsConfiguration.set(
                    MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpHDFS.getRoot().getAbsolutePath());
            miniDFSCluster =
                    new MiniDFSCluster.Builder(hdfsConfiguration)
                            .numDataNodes(1)
                            .waitSafeMode(false)
                            .build();
            miniDFSCluster.waitClusterUp();

            hdfsConfiguration = miniDFSCluster.getConfiguration(0);
            writeHDFSSiteConfigXML(hdfsConfiguration, targetTestClassesFolder);
            YARN_CONFIGURATION.addResource(hdfsConfiguration);
        }
    }

    // write yarn-site.xml to target/test-classes
    public static void writeYarnSiteConfigXML(Configuration yarnConf, File targetFolder)
            throws IOException {
        yarnSiteXML = new File(targetFolder, "/yarn-site.xml");
        try (FileWriter writer = new FileWriter(yarnSiteXML)) {
            yarnConf.writeXml(writer);
            writer.flush();
        }
    }

    // write hdfs-site.xml to target/test-classes
    private static void writeHDFSSiteConfigXML(Configuration coreSite, File targetFolder)
            throws IOException {
        hdfsSiteXML = new File(targetFolder, "/hdfs-site.xml");
        try (FileWriter writer = new FileWriter(hdfsSiteXML)) {
            coreSite.writeXml(writer);
            writer.flush();
        }
    }
}
