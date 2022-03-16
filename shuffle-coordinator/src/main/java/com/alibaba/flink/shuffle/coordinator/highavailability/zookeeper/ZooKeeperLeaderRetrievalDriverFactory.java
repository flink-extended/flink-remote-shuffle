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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriverFactory;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalEventHandler;

import org.apache.curator.framework.CuratorFramework;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/**
 * {@link LeaderRetrievalDriverFactory} implementation for Zookeeper.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriverFactory).
 */
public class ZooKeeperLeaderRetrievalDriverFactory implements LeaderRetrievalDriverFactory {

    private final CuratorFramework client;

    private final String clusterID;

    private final String retrievalPathSuffix;

    private final HaServices.LeaderReceptor receptor;

    public ZooKeeperLeaderRetrievalDriverFactory(
            CuratorFramework client,
            String clusterID,
            String retrievalPathSuffix,
            HaServices.LeaderReceptor receptor) {
        checkArgument(client != null, "Must be not null.");
        checkArgument(clusterID != null, "Must be not null.");
        checkArgument(retrievalPathSuffix != null, "Must be not null.");
        checkArgument(receptor != null, "Must be not null.");

        this.client = client;
        this.clusterID = clusterID;
        this.retrievalPathSuffix = retrievalPathSuffix;
        this.receptor = receptor;
    }

    @Override
    public LeaderRetrievalDriver createLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderEventHandler, FatalErrorHandler fatalErrorHandler)
            throws Exception {
        switch (receptor) {
            case SHUFFLE_CLIENT:
                return new ZooKeeperMultiLeaderRetrievalDriver(
                        client,
                        retrievalPathSuffix,
                        clusterID,
                        leaderEventHandler,
                        fatalErrorHandler);
            case SHUFFLE_WORKER:
                return new ZooKeeperSingleLeaderRetrievalDriver(
                        client,
                        clusterID + retrievalPathSuffix,
                        leaderEventHandler,
                        fatalErrorHandler);
            default:
                throw new ShuffleException("Unknown leader receptor type: " + receptor);
        }
    }
}
