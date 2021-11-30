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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * An implementation of the {@link HaServices} using Apache ZooKeeper. The services store data in
 * ZooKeeper's nodes.
 */
public class ZooKeeperHaServices implements HaServices {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperHaServices.class);

    public static final String SHUFFLE_MANAGER_LEADER_LATCH_PATH = ".shuffle_manager_leader_lock";

    public static final String SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH =
            ".shuffle_manager_leader_info";

    // ------------------------------------------------------------------------

    /** The coordinator configuration. */
    protected final Configuration configuration;

    /** The ZooKeeper client to use. */
    private final CuratorFramework client;

    public ZooKeeperHaServices(Configuration configuration, CuratorFramework client) {
        this.configuration = checkNotNull(configuration);
        this.client = checkNotNull(client);
    }

    @Override
    public LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor) {
        return ZooKeeperUtils.createLeaderRetrievalService(
                client, configuration, SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH, receptor);
    }

    @Override
    public LeaderElectionService createLeaderElectionService() {
        return ZooKeeperUtils.createLeaderElectionService(
                client,
                configuration,
                SHUFFLE_MANAGER_LEADER_LATCH_PATH,
                SHUFFLE_MANAGER_LEADER_RETRIEVAL_PATH);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close {}.", getClass().getSimpleName());

        try {
            client.close();
        } catch (Throwable throwable) {
            LOG.error("Could not properly close {}.", getClass().getSimpleName(), throwable);
            ExceptionUtils.rethrowException(throwable);
        }
    }

    @Override
    public void closeAndCleanupAllData() throws Exception {
        LOG.info("Close and clean up all data for {}.", getClass().getSimpleName());

        Throwable exception = null;

        try {
            cleanupZooKeeperPaths();
        } catch (Throwable throwable) {
            exception = throwable;
        }

        try {
            close();
        } catch (Throwable throwable) {
            exception = exception == null ? throwable : exception;
        }

        if (exception != null) {
            LOG.error(
                    "Could not properly close and clean up all data for {}.",
                    getClass().getSimpleName(),
                    exception);
            ExceptionUtils.rethrowException(exception);
        }

        LOG.info("Finished cleaning up the high availability data.");
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /** Cleans up leftover ZooKeeper paths. */
    private void cleanupZooKeeperPaths() throws Exception {
        deleteOwnedZNode();
        tryDeleteEmptyParentZNodes();
    }

    private void deleteOwnedZNode() throws Exception {
        // delete the HA_CLUSTER_ID znode which is owned by this cluster

        // Since we are using Curator version 2.12 there is a bug in deleting the children
        // if there is a concurrent delete operation. Therefore we need to add this retry
        // logic. See https://issues.apache.org/jira/browse/CURATOR-430 for more information.
        // The retry logic can be removed once we upgrade to Curator version >= 4.0.1.
        boolean zNodeDeleted = false;
        while (!zNodeDeleted) {
            try {
                client.delete().deletingChildrenIfNeeded().forPath("/");
                zNodeDeleted = true;
            } catch (KeeperException.NoNodeException ignored) {
                // concurrent delete operation. Try again.
                LOG.debug(
                        "Retrying to delete owned znode because of other concurrent delete operation.");
            }
        }
    }

    /**
     * Tries to delete empty parent znodes.
     *
     * @throws Exception if the deletion fails for other reason than {@link
     *     KeeperException.NotEmptyException}
     */
    private void tryDeleteEmptyParentZNodes() throws Exception {
        // try to delete the parent znodes if they are empty
        String remainingPath = getParentPath(getNormalizedPath(client.getNamespace()));
        final CuratorFramework nonNamespaceClient = client.usingNamespace(null);

        while (!isRootPath(remainingPath)) {
            try {
                nonNamespaceClient.delete().forPath(remainingPath);
            } catch (KeeperException.NotEmptyException ignored) {
                // We can only delete empty znodes
                break;
            }

            remainingPath = getParentPath(remainingPath);
        }
    }

    private static boolean isRootPath(String remainingPath) {
        return ZKPaths.PATH_SEPARATOR.equals(remainingPath);
    }

    private static String getNormalizedPath(String path) {
        return ZKPaths.makePath(path, "");
    }

    private static String getParentPath(String path) {
        return ZKPaths.getPathAndNode(path).getPath();
    }
}
