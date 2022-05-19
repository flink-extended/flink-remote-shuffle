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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalListener;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to work with {@link LeaderRetrievalService} class.
 *
 * <p>This class is copied from Apache Flink (org.apache.flink.runtime.util.LeaderRetrievalUtils).
 */
public class LeaderRetrievalUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LeaderRetrievalUtils.class);

    /**
     * Retrieves the leader akka url and the current leader session ID. The values are stored in a
     * {@link LeaderConnectionInfo} instance.
     *
     * @param leaderRetrievalService Leader retrieval service to retrieve the leader connection
     *     information
     * @param timeout Timeout when to give up looking for the leader
     * @return LeaderConnectionInfo containing the leader's akka URL and the current leader session
     *     ID
     */
    public static LeaderConnectionInfo retrieveLeaderConnectionInfo(
            LeaderRetrievalService leaderRetrievalService, Duration timeout) throws Exception {

        LeaderConnectionInfoListener listener =
                new LeaderRetrievalUtils.LeaderConnectionInfoListener();

        try {
            leaderRetrievalService.start(listener);

            return listener.getLeaderConnectionInfoFuture()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new Exception(
                    "Could not retrieve the leader address and leader " + "session ID.", e);
        } finally {
            try {
                leaderRetrievalService.stop();
            } catch (Exception fe) {
                LOG.warn("Could not stop the leader retrieval service.", fe);
            }
        }
    }

    public static InetAddress findConnectingAddress(
            LeaderRetrievalService leaderRetrievalService, Duration timeout) throws Exception {

        ConnectionUtils.LeaderConnectingAddressListener listener =
                new ConnectionUtils.LeaderConnectingAddressListener();

        try {
            leaderRetrievalService.start(listener);

            LOG.info(
                    "Trying to select the network interface and address to use "
                            + "by connecting to the leading ShuffleManager.");

            LOG.info(
                    "ShuffleWorker will try to connect for "
                            + timeout
                            + " before falling back to heuristics");

            return listener.findConnectingAddress(timeout);
        } catch (Exception e) {
            throw new Exception(
                    "Could not find the connecting address by connecting to the current leader.",
                    e);
        } finally {
            try {
                leaderRetrievalService.stop();
            } catch (Exception fe) {
                LOG.warn("Could not stop the leader retrieval service.", fe);
            }
        }
    }

    /**
     * Helper class which is used by the retrieveLeaderConnectionInfo method to retrieve the
     * leader's akka URL and the current leader session ID.
     */
    public static class LeaderConnectionInfoListener implements LeaderRetrievalListener {
        private final CompletableFuture<LeaderConnectionInfo> connectionInfoFuture =
                new CompletableFuture<>();

        public CompletableFuture<LeaderConnectionInfo> getLeaderConnectionInfoFuture() {
            return connectionInfoFuture;
        }

        @Override
        public void notifyLeaderAddress(LeaderInformation leaderInfo) {
            String leaderAddress = leaderInfo.getLeaderAddress();
            UUID leaderSessionID = leaderInfo.getLeaderSessionID();
            if (leaderAddress != null
                    && !leaderAddress.equals("")
                    && !connectionInfoFuture.isDone()) {
                final LeaderConnectionInfo leaderConnectionInfo =
                        new LeaderConnectionInfo(leaderSessionID, leaderAddress);
                connectionInfoFuture.complete(leaderConnectionInfo);
            }
        }

        @Override
        public void handleError(Exception exception) {
            connectionInfoFuture.completeExceptionally(exception);
        }
    }

    // ------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private LeaderRetrievalUtils() {
        throw new RuntimeException();
    }
}
