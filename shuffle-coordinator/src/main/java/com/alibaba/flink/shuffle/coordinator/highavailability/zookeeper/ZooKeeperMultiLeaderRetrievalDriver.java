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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
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

    private final String retrievalPathPrefix;

    private final LeaderRetrievalEventHandler leaderListener;

    private final PathChildrenCache pathChildrenCache;

    private final FatalErrorHandler fatalErrorHandler;

    private final AtomicReference<String> currentLeaderPath = new AtomicReference<>();

    private final AtomicBoolean running = new AtomicBoolean(true);

    private final AtomicBoolean hasInitialized = new AtomicBoolean(false);

    public ZooKeeperMultiLeaderRetrievalDriver(
            CuratorFramework client,
            String retrievalPathSuffix,
            String retrievalPathPrefix,
            LeaderRetrievalEventHandler leaderListener,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        checkArgument(client != null, "Must be not null.");
        checkArgument(retrievalPathSuffix != null, "Must be not null.");
        checkArgument(retrievalPathPrefix != null, "Must be not null.");
        checkArgument(retrievalPathPrefix.startsWith("/"), "Path must start with '/'.");
        checkArgument(leaderListener != null, "Must be not null.");
        checkArgument(fatalErrorHandler != null, "Must be not null.");

        this.client = client;
        this.retrievalPathSuffix = retrievalPathSuffix;
        this.retrievalPathPrefix = retrievalPathPrefix;
        this.leaderListener = leaderListener;
        this.fatalErrorHandler = fatalErrorHandler;

        client.getUnhandledErrorListenable().addListener(this);
        this.pathChildrenCache = new PathChildrenCache(client, "/", true);
        pathChildrenCache.getListenable().addListener(this);
        pathChildrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
    }

    private void mayUpdateLeader(List<ChildData> childDataList) {
        if (childDataList == null || childDataList.isEmpty()) {
            return;
        }

        LeaderInformation selectedLeaderInfo = null;
        String selectedLeaderPath = null;
        String leaderPath = currentLeaderPath.get();
        try {
            for (ChildData childData : childDataList) {
                if (childData == null
                        || !childData.getPath().endsWith(retrievalPathSuffix)
                        || !childData.getPath().startsWith(retrievalPathPrefix)) {
                    continue;
                }

                LeaderInformation leaderInfo = deserializeLeaderInfo(childData);
                if (leaderInfo == null) {
                    continue;
                }

                if (childData.getPath().equals(leaderPath)) {
                    // always select the old leader if exist
                    selectedLeaderInfo = leaderInfo;
                    selectedLeaderPath = childData.getPath();
                    break;
                } else if (selectedLeaderInfo == null
                        || selectedLeaderInfo.getProtocolVersion()
                                < leaderInfo.getProtocolVersion()) {
                    selectedLeaderInfo = leaderInfo;
                    selectedLeaderPath = childData.getPath();
                }
            }

            if (selectedLeaderInfo != null) {
                currentLeaderPath.set(selectedLeaderPath);
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
        boolean initialized = hasInitialized.get();

        switch (pathChildrenCacheEvent.getType()) {
            case INITIALIZED:
                List<ChildData> initialData = pathChildrenCacheEvent.getInitialData();
                LOG.info(
                        "Children cache initialized, "
                                + "begin to retrieve leader information: {} - {} - {}.",
                        initialData == null ? 0 : initialData.size(),
                        leaderPath,
                        initialized);
                hasInitialized.set(true);
                mayUpdateLeader(initialData);
                break;
            case CHILD_ADDED:
                LOG.info(
                        "New child node added: {} - {} - {}.",
                        childData.getPath(),
                        leaderPath,
                        initialized);
                if (!initialized
                        || (leaderPath != null && !leaderPath.equals(childData.getPath()))) {
                    return;
                }
                mayUpdateLeader(Collections.singletonList(childData));
                break;
            case CHILD_REMOVED:
                LOG.info(
                        "Child node removed: {} - {} - {}.",
                        childData.getPath(),
                        leaderPath,
                        initialized);
                if (!initialized || leaderPath == null || !leaderPath.equals(childData.getPath())) {
                    return;
                }
                mayUpdateLeader(pathChildrenCache.getCurrentData());
                break;
            case CHILD_UPDATED:
                LOG.info(
                        "Child node data updated: {} - {} - {}.",
                        childData.getPath(),
                        leaderPath,
                        initialized);
                if (!initialized || leaderPath == null || !leaderPath.equals(childData.getPath())) {
                    return;
                }
                mayUpdateLeader(Collections.singletonList(childData));
                break;
            case CONNECTION_SUSPENDED:
                LOG.warn(
                        "Connection to ZooKeeper suspended. "
                                + "Can no longer retrieve the leader: {} - {}.",
                        leaderPath,
                        initialized);
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                break;
            case CONNECTION_RECONNECTED:
                LOG.info(
                        "Connection to ZooKeeper was reconnected. "
                                + "Restart leader retrieval: {} - {}.",
                        leaderPath,
                        initialized);
                mayUpdateLeader(pathChildrenCache.getCurrentData());
                break;
            case CONNECTION_LOST:
                LOG.warn(
                        "Connection to ZooKeeper lost. Can no longer retrieve the leader: {} - {}.",
                        leaderPath,
                        initialized);
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                break;
            default:
                // this should never happen
                unhandledError(
                        "Unknown zookeeper event: " + pathChildrenCacheEvent.getType(), null);
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
