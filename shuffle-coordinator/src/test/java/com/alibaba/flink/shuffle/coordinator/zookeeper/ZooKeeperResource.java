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

package com.alibaba.flink.shuffle.coordinator.zookeeper;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

/** {@link ExternalResource} which starts a {@link org.apache.zookeeper.server.ZooKeeperServer}. */
public class ZooKeeperResource extends ExternalResource {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperResource.class);

    @Nullable private TestingServer zooKeeperServer;

    public String getConnectString() {
        verifyIsRunning();
        return zooKeeperServer.getConnectString();
    }

    private void verifyIsRunning() {
        CommonUtils.checkState(zooKeeperServer != null);
    }

    @Override
    protected void before() throws Throwable {
        terminateZooKeeperServer();
        zooKeeperServer = new TestingServer(true);
    }

    private void terminateZooKeeperServer() throws IOException {
        if (zooKeeperServer != null) {
            zooKeeperServer.stop();
            zooKeeperServer = null;
        }
    }

    @Override
    protected void after() {
        try {
            terminateZooKeeperServer();
        } catch (IOException e) {
            LOG.warn("Could not properly terminate the {}.", getClass().getSimpleName(), e);
        }
    }

    public void restart() throws Exception {
        CommonUtils.checkNotNull(zooKeeperServer);
        zooKeeperServer.restart();
    }
}
