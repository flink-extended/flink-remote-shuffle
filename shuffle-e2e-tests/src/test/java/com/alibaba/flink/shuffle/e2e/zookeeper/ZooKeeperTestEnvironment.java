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

package com.alibaba.flink.shuffle.e2e.zookeeper;

import org.apache.curator.test.TestingServer;

/** Simple ZooKeeper and CuratorFramework setup for tests. */
public class ZooKeeperTestEnvironment {

    private final TestingServer zooKeeperServer;

    /** Starts a ZooKeeper server and a client. */
    public ZooKeeperTestEnvironment() {
        try {
            zooKeeperServer = new TestingServer(true);
        } catch (Exception e) {
            throw new RuntimeException("Error setting up ZooKeeperTestEnvironment", e);
        }
    }

    public void restart() throws Exception {
        zooKeeperServer.restart();
    }

    /** Shutdown the client and ZooKeeper server/cluster. */
    public void shutdown() throws Exception {
        zooKeeperServer.close();
    }

    public String getConnect() {
        return zooKeeperServer.getConnectString();
    }
}
