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

package com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper;

import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionDriverFactory;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionEventHandler;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

/**
 * {@link LeaderElectionDriverFactory} implementation for Zookeeper.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderElectionDriverFactory).
 */
public class ZooKeeperLeaderElectionDriverFactory implements LeaderElectionDriverFactory {

    private final CuratorFramework client;

    private final String latchPath;

    private final String leaderPath;

    public ZooKeeperLeaderElectionDriverFactory(
            CuratorFramework client, String latchPath, String leaderPath) {
        this.client = client;
        this.latchPath = latchPath;
        this.leaderPath = leaderPath;
    }

    @Override
    public ZooKeeperLeaderElectionDriver createLeaderElectionDriver(
            LeaderElectionEventHandler leaderEventHandler,
            FatalErrorHandler fatalErrorHandler,
            String leaderContenderDescription)
            throws Exception {
        return new ZooKeeperLeaderElectionDriver(
                client,
                latchPath,
                leaderPath,
                leaderEventHandler,
                fatalErrorHandler,
                leaderContenderDescription);
    }
}
