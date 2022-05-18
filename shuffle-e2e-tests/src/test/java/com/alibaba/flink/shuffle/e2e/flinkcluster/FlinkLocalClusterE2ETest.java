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

package com.alibaba.flink.shuffle.e2e.flinkcluster;

import com.alibaba.flink.shuffle.e2e.utils.CommonTestUtils;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestEnvironment;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.time.Duration;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link FlinkLocalCluster}. */
public class FlinkLocalClusterE2ETest {

    @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public TestName name = new TestName();

    private ZooKeeperTestEnvironment zkCluster;

    private FlinkLocalCluster cluster;

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

        zkCluster = new ZooKeeperTestEnvironment(1);
        cluster =
                new FlinkLocalCluster(
                        logDir.getAbsolutePath(),
                        2,
                        temporaryFolder,
                        zkCluster.getConnect(),
                        new Configuration());
    }

    @After
    public void cleanup() throws Exception {
        cluster.shutdown();
        zkCluster.shutdown();
    }

    @Test
    public void testRecoverTaskManager() throws Exception {
        cluster.start();

        assertThat(cluster.getNumTaskManagersConnected(), is(2));

        cluster.killTaskManager(0);
        CommonTestUtils.waitUntilCondition(
                () -> cluster.getNumTaskManagersConnected() == 1,
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout.");
        assertThat(cluster.isTaskManagerAlive(0), is(false));

        // recover
        cluster.recoverTaskManager(0);
        CommonTestUtils.waitUntilCondition(
                () -> cluster.getNumTaskManagersConnected() == 2,
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout.");
        assertThat(cluster.isTaskManagerAlive(0), is(true));
    }
}
