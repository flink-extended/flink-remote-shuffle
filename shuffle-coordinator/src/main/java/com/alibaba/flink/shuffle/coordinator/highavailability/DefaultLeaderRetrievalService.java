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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.Objects;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * The counterpart to the {@link DefaultLeaderElectionService}. Composed with different {@link
 * LeaderRetrievalDriver}, we could retrieve the leader information from different storage. The
 * leader address as well as the current leader session ID will be retrieved from {@link
 * LeaderRetrievalDriver}.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService).
 */
public class DefaultLeaderRetrievalService
        implements LeaderRetrievalService, LeaderRetrievalEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderRetrievalService.class);

    private final Object lock = new Object();

    private final LeaderRetrievalDriverFactory leaderRetrievalDriverFactory;

    @GuardedBy("lock")
    private LeaderInformation lastLeaderInfo;

    @GuardedBy("lock")
    private boolean running;

    /** Listener which will be notified about leader changes. */
    @GuardedBy("lock")
    private LeaderRetrievalListener leaderListener;

    @GuardedBy("lock")
    private LeaderRetrievalDriver leaderRetrievalDriver;

    public DefaultLeaderRetrievalService(
            LeaderRetrievalDriverFactory leaderRetrievalDriverFactory) {
        checkArgument(leaderRetrievalDriverFactory != null, "Must be not null.");
        this.leaderRetrievalDriverFactory = leaderRetrievalDriverFactory;
        this.lastLeaderInfo = LeaderInformation.empty();
    }

    @Override
    public void start(LeaderRetrievalListener listener) throws Exception {
        checkArgument(listener != null, "Listener must not be null.");

        synchronized (lock) {
            checkState(
                    leaderListener == null,
                    "DefaultLeaderRetrievalService can only be started once.");
            running = true;
            leaderListener = listener;
            leaderRetrievalDriver =
                    leaderRetrievalDriverFactory.createLeaderRetrievalDriver(
                            this,
                            new DefaultLeaderRetrievalService.LeaderRetrievalFatalErrorHandler());
            LOG.info("Starting DefaultLeaderRetrievalService with {}.", leaderRetrievalDriver);
        }
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping DefaultLeaderRetrievalService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;

            leaderRetrievalDriver.close();
        }
    }

    /**
     * Called by specific {@link LeaderRetrievalDriver} to notify leader address.
     *
     * @param leaderInfo New notified leader information. The exception will be handled by leader
     *     listener.
     */
    @Override
    public void notifyLeaderAddress(LeaderInformation leaderInfo) {
        synchronized (lock) {
            if (!running) {
                LOG.debug(
                        "Ignoring notification since the {} has already been closed.",
                        leaderRetrievalDriver);
                return;
            }

            LOG.info("New leader information: {}.", leaderInfo);
            if (!Objects.equals(lastLeaderInfo, leaderInfo)) {
                lastLeaderInfo = leaderInfo;
                // Notify the listener only when the leader is truly changed.
                leaderListener.notifyLeaderAddress(leaderInfo);
            }
        }
    }

    private class LeaderRetrievalFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            synchronized (lock) {
                if (!running) {
                    LOG.debug("Ignoring error notification since the service has been stopped.");
                    return;
                }
                leaderListener.handleError(new Exception(throwable));
            }
        }
    }
}
