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

package com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.DefaultLeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.DefaultLeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionDriverFactory;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriverFactory;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Class containing helper functions to interact with ZooKeeper. */
public class ZooKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

    /**
     * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper quorum.
     *
     * @param configuration {@link Configuration} object containing the configuration values.
     */
    public static CuratorFramework startCuratorFramework(Configuration configuration) {
        checkNotNull(configuration);
        String zkQuorum = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);

        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new RuntimeException(
                    "No valid ZooKeeper quorum has been specified. "
                            + "You can specify the quorum via the configuration key '"
                            + HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key()
                            + "'.");
        }

        long sessionTimeout =
                configuration
                        .getDuration(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT)
                        .toMillis();
        long connectionTimeout =
                configuration
                        .getDuration(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT)
                        .toMillis();
        long retryWait =
                configuration.getDuration(HighAvailabilityOptions.ZOOKEEPER_RETRY_WAIT).toMillis();
        int maxRetryAttempts =
                configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

        String root = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);
        LOG.info("Using '{}' as Zookeeper namespace.", root);

        CuratorFramework curatorFramework =
                CuratorFrameworkFactory.builder()
                        .connectString(zkQuorum)
                        .sessionTimeoutMs(CommonUtils.checkedDownCast(sessionTimeout))
                        .connectionTimeoutMs(CommonUtils.checkedDownCast(connectionTimeout))
                        .retryPolicy(
                                new ExponentialBackoffRetry(
                                        CommonUtils.checkedDownCast(retryWait), maxRetryAttempts))
                        // Curator prepends a '/' manually and throws an Exception if the
                        // namespace starts with a '/'.
                        .namespace(root.startsWith("/") ? root.substring(1) : root)
                        .aclProvider(new DefaultACLProvider())
                        .build();
        curatorFramework.start();
        return curatorFramework;
    }

    /**
     * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
     * tolerate it).
     */
    public static String getZooKeeperEnsemble(Configuration conf) {
        String zkQuorum = conf.getString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);
        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new ConfigurationException("No ZooKeeper quorum specified in config.");
        }
        // Remove all whitespace
        return zkQuorum.replaceAll("\\s+", "");
    }

    /**
     * Creates a {@link DefaultLeaderRetrievalService} instance with {@link
     * ZooKeeperSingleLeaderRetrievalDriver}.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param retrievalPathSuffix The path suffix of the leader retrieval node
     * @param receptor Type of the leader information receptor
     * @return {@link DefaultLeaderRetrievalService} instance.
     */
    public static DefaultLeaderRetrievalService createLeaderRetrievalService(
            final CuratorFramework client,
            final Configuration configuration,
            final String retrievalPathSuffix,
            final HaServices.LeaderReceptor receptor) {
        return new DefaultLeaderRetrievalService(
                createLeaderRetrievalDriverFactory(
                        client, configuration, retrievalPathSuffix, receptor));
    }

    /**
     * Creates a {@link LeaderRetrievalDriverFactory} implemented by ZooKeeper.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param retrievalPathSuffix The path suffix of the leader retrieval node
     * @param receptor Type of the leader information receptor
     * @return {@link LeaderRetrievalDriverFactory} instance.
     */
    public static ZooKeeperLeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(
            final CuratorFramework client,
            final Configuration configuration,
            final String retrievalPathSuffix,
            final HaServices.LeaderReceptor receptor) {
        return new ZooKeeperLeaderRetrievalDriverFactory(
                client, generateZookeeperClusterId(configuration), retrievalPathSuffix, receptor);
    }

    /**
     * Creates a {@link DefaultLeaderElectionService} instance with {@link
     * ZooKeeperLeaderElectionDriver}.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param latchPathSuffix The path suffix of the leader latch node
     * @param retrievalPathSuffix The path suffix of the leader retrieval node
     * @return {@link DefaultLeaderElectionService} instance.
     */
    public static DefaultLeaderElectionService createLeaderElectionService(
            final CuratorFramework client,
            final Configuration configuration,
            final String latchPathSuffix,
            final String retrievalPathSuffix) {
        return new DefaultLeaderElectionService(
                createLeaderElectionDriverFactory(
                        client, configuration, latchPathSuffix, retrievalPathSuffix));
    }

    /**
     * Creates a {@link LeaderElectionDriverFactory} implemented by ZooKeeper.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object containing the configuration values
     * @param latchPathSuffix The path suffix of the leader latch node
     * @param retrievalPathSuffix The path suffix of the leader retrieval node
     * @return {@link LeaderElectionDriverFactory} instance.
     */
    public static ZooKeeperLeaderElectionDriverFactory createLeaderElectionDriverFactory(
            final CuratorFramework client,
            final Configuration configuration,
            final String latchPathSuffix,
            final String retrievalPathSuffix) {
        return new ZooKeeperLeaderElectionDriverFactory(
                client,
                getPath(configuration, latchPathSuffix),
                getPath(configuration, retrievalPathSuffix));
    }

    private static String getPath(Configuration configuration, String pathSuffix) {
        return generateZookeeperClusterId(configuration) + pathSuffix;
    }

    private static String generateZookeeperClusterId(Configuration configuration) {
        String clusterId = configuration.getString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID);
        if (clusterId.trim().isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "Illegal config value for %s. Must be not empty.",
                            ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.key()));
        }

        if (!clusterId.startsWith("/")) {
            clusterId = '/' + clusterId;
        }

        if (clusterId.endsWith("/")) {
            clusterId = clusterId.substring(0, clusterId.length() - 1);
        }

        return clusterId;
    }

    /** Private constructor to prevent instantiation. */
    private ZooKeeperUtils() {}
}
