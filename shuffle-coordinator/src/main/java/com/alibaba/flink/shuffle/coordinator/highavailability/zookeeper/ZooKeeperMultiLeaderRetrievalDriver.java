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
import com.alibaba.flink.shuffle.common.utils.ProtocolUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalEventHandler;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.flink.shaded.curator4.org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/**
 * A {@link LeaderRetrievalDriver} implementation which can select a leader from multiple remote
 * shuffle clusters.
 */
public class ZooKeeperMultiLeaderRetrievalDriver
        implements LeaderRetrievalDriver, UnhandledErrorListener, PathChildrenCacheListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperMultiLeaderRetrievalDriver.class);

    private final CuratorFramework client;

    private final String retrievalPathSuffix;

    private final LeaderRetrievalEventHandler leaderListener;

    private final PathChildrenCache pathChildrenCache;

    private final FatalErrorHandler fatalErrorHandler;

    private final AtomicReference<String> currentLeaderPath = new AtomicReference<>();

    private final AtomicBoolean running = new AtomicBoolean(true);

    public ZooKeeperMultiLeaderRetrievalDriver(
            CuratorFramework client,
            String retrievalPathSuffix,
            LeaderRetrievalEventHandler leaderListener,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        checkArgument(client != null, "Must be not null.");
        checkArgument(retrievalPathSuffix != null, "Must be not null.");
        checkArgument(leaderListener != null, "Must be not null.");
        checkArgument(fatalErrorHandler != null, "Must be not null.");

        this.client = client;
        this.retrievalPathSuffix = retrievalPathSuffix;
        this.leaderListener = leaderListener;
        this.fatalErrorHandler = fatalErrorHandler;

        client.getUnhandledErrorListenable().addListener(this);
        this.pathChildrenCache = new PathChildrenCache(client, "/", true);
        pathChildrenCache.getListenable().addListener(this);
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        mayUpdateLeader(pathChildrenCache.getCurrentData());
    }

    private void mayUpdateLeader(List<ChildData> childDataList) {
        if (childDataList == null || childDataList.isEmpty() || currentLeaderPath.get() != null) {
            return;
        }

        LeaderInformation selectedLeaderInfo = null;
        String selectedLeaderPath = null;
        try {
            for (ChildData childData : childDataList) {
                if (childData == null || !childData.getPath().endsWith(retrievalPathSuffix)) {
                    continue;
                }

                LeaderInformation leaderInfo = deserializeLeaderInfo(childData);
                if (leaderInfo != null
                        && (selectedLeaderInfo == null
                                || selectedLeaderInfo.getProtocolVersion()
                                        < leaderInfo.getProtocolVersion())) {
                    selectedLeaderInfo = leaderInfo;
                    selectedLeaderPath = childData.getPath();
                }
            }

            if (selectedLeaderInfo != null
                    && currentLeaderPath.compareAndSet(null, selectedLeaderPath)) {
                notifyNewLeaderInfo(selectedLeaderInfo);
            }
        } catch (Throwable throwable) {
            fatalErrorHandler.onFatalError(
                    new Exception("FATAL: Failed to retrieve the leader information.", throwable));
        }
    }

    private LeaderInformation deserializeLeaderInfo(ChildData childData) {
        try {
            if (childData == null) {
                return null;
            }

            byte[] dataBytes = childData.getData();
            if (dataBytes == null || dataBytes.length == 0) {
                return null;
            }

            LeaderInformation leaderInfo = LeaderInformation.fromByteArray(dataBytes);
            if (!ProtocolUtils.isServerProtocolCompatible(
                    leaderInfo.getProtocolVersion(), leaderInfo.getSupportedVersion())) {
                LOG.info("Ignore incompatible leader {}.", leaderInfo);
                return null;
            }
            return leaderInfo;
        } catch (Throwable throwable) {
            fatalErrorHandler.onFatalError(
                    new Exception("FATAL: Failed to deserialize leader information.", throwable));
            return null;
        }
    }

    private void notifyNewLeaderInfo(LeaderInformation leaderInfo) {
        LOG.info("Notify new leader information: {} : {}.", leaderInfo, currentLeaderPath.get());
        leaderListener.notifyLeaderAddress(leaderInfo);
    }

    @Override
    public void childEvent(
            CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) {
        String leaderPath = currentLeaderPath.get();
        ChildData childData = pathChildrenCacheEvent.getData();
        LeaderInformation leaderInfo;

        switch (pathChildrenCacheEvent.getType()) {
            case INITIALIZED:
                LOG.info("Children cache initialized, begin to retrieve leader information.");
                break;
            case CHILD_ADDED:
                LOG.info("New child node added: {}.", childData.getPath());
                mayUpdateLeader(Collections.singletonList(childData));
                break;
            case CHILD_REMOVED:
                LOG.info("New child node added: {}.", childData.getPath());
                if (leaderPath == null || !leaderPath.equals(childData.getPath())) {
                    return;
                }

                if (currentLeaderPath.compareAndSet(leaderPath, null)) {
                    notifyNewLeaderInfo(LeaderInformation.empty());
                }
                mayUpdateLeader(pathChildrenCache.getCurrentData());
                break;
            case CHILD_UPDATED:
                LOG.info("Child node data updated: {}.", childData.getPath());
                if (leaderPath == null || !leaderPath.equals(childData.getPath())) {
                    return;
                }

                leaderInfo = deserializeLeaderInfo(childData);
                notifyNewLeaderInfo(leaderInfo != null ? leaderInfo : LeaderInformation.empty());
                break;
            case CONNECTION_SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended. Can no longer retrieve the leader.");
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                break;
            case CONNECTION_RECONNECTED:
                LOG.info("Connection to ZooKeeper was reconnected. Restart leader retrieval.");
                currentLeaderPath.compareAndSet(leaderPath, null);
                mayUpdateLeader(pathChildrenCache.getCurrentData());
                break;
            case CONNECTION_LOST:
                LOG.warn("Connection to ZooKeeper lost. Can no longer retrieve the leader.");
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                break;
            default:
                // this should never happen
                fatalErrorHandler.onFatalError(
                        new Exception(
                                "Unknown zookeeper event: " + pathChildrenCacheEvent.getType()));
        }
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            LOG.info("Closing {}.", this);

            client.getUnhandledErrorListenable().removeListener(this);

            try {
                pathChildrenCache.close();
            } catch (Throwable throwable) {
                throw new Exception(
                        "Could not properly stop the ZooKeeperLeaderRetrievalDriver.", throwable);
            }
        }
    }

    @Override
    public void unhandledError(String message, Throwable throwable) {
        fatalErrorHandler.onFatalError(
                new Exception(
                        String.format(
                                "Unhandled error in ZooKeeperMultiLeaderRetrievalDriver: %s.",
                                message),
                        throwable));
    }

    @Override
    public String toString() {
        return "ZooKeeperMultiLeaderRetrievalDriver{"
                + "retrievalPathSuffix="
                + retrievalPathSuffix
                + "}";
    }
}
