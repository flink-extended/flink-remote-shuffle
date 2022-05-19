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

package com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper;

import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalEventHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/**
 * The counterpart to the {@link ZooKeeperLeaderElectionDriver}. {@link LeaderRetrievalService}
 * implementation for Zookeeper. It retrieves the current leader which has been elected by the
 * {@link ZooKeeperLeaderElectionDriver}. The leader address as well as the current leader session
 * ID is retrieved from ZooKeeper.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver).
 */
public class ZooKeeperSingleLeaderRetrievalDriver
        implements LeaderRetrievalDriver, NodeCacheListener, UnhandledErrorListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(ZooKeeperSingleLeaderRetrievalDriver.class);

    /** Connection to the used ZooKeeper quorum. */
    private final CuratorFramework client;

    /** Curator recipe to watch changes of a specific ZooKeeper node. */
    private final NodeCache nodeCache;

    private final String retrievalPath;

    private final ConnectionStateListener connectionStateListener =
            (client, newState) -> handleStateChange(newState);

    private final LeaderRetrievalEventHandler leaderListener;

    private final FatalErrorHandler fatalErrorHandler;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public ZooKeeperSingleLeaderRetrievalDriver(
            CuratorFramework client,
            String retrievalPath,
            LeaderRetrievalEventHandler leaderListener,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {
        checkArgument(client != null, "CuratorFramework client must be not null.");
        checkArgument(retrievalPath != null, "Retrieval path must be not null.");
        checkArgument(leaderListener != null, "Event handler must be not null.");
        checkArgument(fatalErrorHandler != null, "Fatal error handler must be not null.");

        this.client = client;
        this.nodeCache = new NodeCache(client, retrievalPath);
        this.retrievalPath = retrievalPath;
        this.leaderListener = leaderListener;
        this.fatalErrorHandler = fatalErrorHandler;

        client.getUnhandledErrorListenable().addListener(this);
        nodeCache.getListenable().addListener(this);
        nodeCache.start();
        client.getConnectionStateListenable().addListener(connectionStateListener);
    }

    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            LOG.info("Closing {}.", this);

            client.getUnhandledErrorListenable().removeListener(this);
            client.getConnectionStateListenable().removeListener(connectionStateListener);

            try {
                nodeCache.close();
            } catch (Throwable throwable) {
                throw new Exception(
                        "Could not properly stop the ZooKeeperLeaderRetrievalDriver.", throwable);
            }
        }
    }

    @Override
    public void nodeChanged() {
        LOG.info("Leader node has changed.");
        retrieveLeaderInformationFromZooKeeper();
    }

    private void retrieveLeaderInformationFromZooKeeper() {
        try {
            ChildData childData = nodeCache.getCurrentData();
            if (childData == null) {
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                return;
            }

            byte[] data = childData.getData();
            if (data == null || data.length <= 0) {
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                return;
            }

            LeaderInformation leaderInfo = LeaderInformation.fromByteArray(data);
            leaderListener.notifyLeaderAddress(leaderInfo);
        } catch (Throwable throwable) {
            fatalErrorHandler.onFatalError(
                    new Exception("Could not handle node changed event.", throwable));
        }
    }

    private void handleStateChange(ConnectionState newState) {
        switch (newState) {
            case CONNECTED:
                LOG.info("Connected to ZooKeeper quorum. Leader retrieval can start.");
                break;
            case SUSPENDED:
                LOG.warn("Connection to ZooKeeper suspended. Can no longer retrieve the leader.");
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                break;
            case RECONNECTED:
                LOG.info("Connection to ZooKeeper was reconnected. Restart leader retrieval.");
                retrieveLeaderInformationFromZooKeeper();
                break;
            case LOST:
                LOG.warn("Connection to ZooKeeper lost. Can no longer retrieve the leader.");
                leaderListener.notifyLeaderAddress(LeaderInformation.empty());
                break;
        }
    }

    @Override
    public void unhandledError(String message, Throwable throwable) {
        fatalErrorHandler.onFatalError(
                new Exception(
                        String.format(
                                "Unhandled error in ZooKeeperSingleLeaderRetrievalDriver: %s.",
                                message),
                        throwable));
    }

    @Override
    public String toString() {
        return "ZooKeeperSingleLeaderRetrievalDriver{" + "retrievalPath=" + retrievalPath + "}";
    }
}
