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

package com.alibaba.flink.shuffle.yarn.zk;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** A class to start a Zookeeper {@link TestingServer}. */
public class ZookeeperTestServer extends ExternalResource {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperTestServer.class);

    @Nullable private TestingServer zooKeeperServer;

    public String getConnectString() throws Exception {
        initTestingServer();
        verifyIsRunning();
        return zooKeeperServer.getConnectString();
    }

    private void verifyIsRunning() {
        CommonUtils.checkState(zooKeeperServer != null);
    }

    private void initTestingServer() throws Exception {
        if (zooKeeperServer == null) {
            zooKeeperServer = new TestingServer(true);
        }
    }

    private void terminateZooKeeperServer() throws IOException {
        if (zooKeeperServer != null) {
            zooKeeperServer.stop();
            zooKeeperServer = null;
        }
    }

    public void afterOperations() {
        try {
            terminateZooKeeperServer();
        } catch (IOException e) {
            LOG.warn("Could not terminate the zookeeper server properly, ", e);
        }
    }
}
