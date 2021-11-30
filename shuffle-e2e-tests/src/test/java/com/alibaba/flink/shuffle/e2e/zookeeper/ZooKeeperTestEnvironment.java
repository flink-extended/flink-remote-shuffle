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

package com.alibaba.flink.shuffle.e2e.zookeeper;

import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.TestingServer;

/** Simple ZooKeeper and CuratorFramework setup for tests. */
public class ZooKeeperTestEnvironment {

    private final TestingServer zooKeeperServer;
    private final TestingCluster zooKeeperCluster;

    /**
     * Starts a ZooKeeper cluster with the number of quorum peers and a client.
     *
     * @param numberOfZooKeeperQuorumPeers Starts a {@link TestingServer}, if <code>1</code>. Starts
     *     a {@link TestingCluster}, if <code>=>1</code>.
     */
    public ZooKeeperTestEnvironment(int numberOfZooKeeperQuorumPeers) {
        if (numberOfZooKeeperQuorumPeers <= 0) {
            throw new IllegalArgumentException("Number of peers needs to be >= 1.");
        }

        try {
            if (numberOfZooKeeperQuorumPeers == 1) {
                zooKeeperServer = new TestingServer(true);
                zooKeeperCluster = null;
            } else {
                zooKeeperServer = null;
                zooKeeperCluster = new TestingCluster(numberOfZooKeeperQuorumPeers);

                zooKeeperCluster.start();
            }
        } catch (Exception e) {
            throw new RuntimeException("Error setting up ZooKeeperTestEnvironment", e);
        }
    }

    /** Shutdown the client and ZooKeeper server/cluster. */
    public void shutdown() throws Exception {

        if (zooKeeperServer != null) {
            zooKeeperServer.close();
        }

        if (zooKeeperCluster != null) {
            zooKeeperCluster.close();
        }
    }

    public String getConnect() {
        if (zooKeeperServer != null) {
            return zooKeeperServer.getConnectString();
        } else {
            return zooKeeperCluster.getConnectString();
        }
    }
}
