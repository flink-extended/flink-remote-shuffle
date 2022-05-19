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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.util.UUID;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * Default implementation for leader election service. Composed with different {@link
 * LeaderElectionDriver}, we could perform a leader election for the contender, and then persist the
 * leader information to various storage.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderelection.DefaultLeaderElectionService).
 */
public class DefaultLeaderElectionService
        implements LeaderElectionService, LeaderElectionEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLeaderElectionService.class);

    private final Object lock = new Object();

    private final LeaderElectionDriverFactory leaderElectionDriverFactory;

    /** The leader contender which applies for leadership. */
    @GuardedBy("lock")
    private LeaderContender leaderContender;

    @GuardedBy("lock")
    private UUID issuedLeaderSessionID;

    @GuardedBy("lock")
    private LeaderInformation confirmedLeaderInfo;

    @GuardedBy("lock")
    private boolean running;

    @GuardedBy("lock")
    private LeaderElectionDriver leaderElectionDriver;

    public DefaultLeaderElectionService(LeaderElectionDriverFactory leaderElectionDriverFactory) {
        checkArgument(leaderElectionDriverFactory != null, "Must be not null.");
        this.leaderElectionDriverFactory = leaderElectionDriverFactory;
    }

    @Override
    public final void start(LeaderContender contender) throws Exception {
        checkArgument(contender != null, "Contender must not be null.");

        synchronized (lock) {
            checkState(leaderContender == null, "Contender was already set.");
            leaderContender = contender;
            leaderElectionDriver =
                    leaderElectionDriverFactory.createLeaderElectionDriver(
                            this,
                            new LeaderElectionFatalErrorHandler(),
                            leaderContender.getDescription());
            running = true;
            LOG.info("Starting DefaultLeaderElectionService with {}.", leaderElectionDriver);
        }
    }

    @Override
    public final void stop() throws Exception {
        LOG.info("Stopping DefaultLeaderElectionService.");

        synchronized (lock) {
            if (!running) {
                return;
            }
            running = false;

            clearConfirmedLeaderInformation();
            leaderElectionDriver.close();
        }
    }

    @Override
    public void confirmLeadership(LeaderInformation leaderInfo) {
        LOG.info("Confirm leader {}.", leaderInfo);
        UUID leaderSessionID = checkNotNull(leaderInfo.getLeaderSessionID());

        synchronized (lock) {
            if (hasLeadership(leaderSessionID)) {
                if (running) {
                    confirmLeaderInformation(leaderInfo);
                    return;
                }
                LOG.debug(
                        "Ignoring the leader session Id {} confirmation, since the "
                                + "LeaderElectionService has already been stopped.",
                        leaderSessionID);
                return;
            }

            // Received an old confirmation call
            if (!leaderSessionID.equals(issuedLeaderSessionID)) {
                LOG.warn(
                        "Receive an old confirmation call of leader session ID {}, current "
                                + "issued session ID is {}",
                        leaderSessionID,
                        issuedLeaderSessionID);
            } else {
                LOG.warn(
                        "The leader session ID {} was confirmed even though the "
                                + "corresponding contender was not elected as the leader.",
                        leaderSessionID);
            }
        }
    }

    @Override
    public boolean hasLeadership(UUID leaderSessionId) {
        synchronized (lock) {
            if (running) {
                return leaderElectionDriver.hasLeadership()
                        && leaderSessionId.equals(issuedLeaderSessionID);
            }
            LOG.debug("hasLeadership is called after the service is stopped, returning false.");
            return false;
        }
    }

    private void confirmLeaderInformation(LeaderInformation leaderInfo) {
        assert Thread.holdsLock(lock);
        confirmedLeaderInfo = leaderInfo;
        leaderElectionDriver.writeLeaderInformation(leaderInfo);
    }

    private void clearConfirmedLeaderInformation() {
        assert Thread.holdsLock(lock);
        confirmedLeaderInfo = null;
    }

    @Override
    public void onGrantLeadership() {
        synchronized (lock) {
            if (!running) {
                LOG.warn(
                        "Ignoring the grant leadership notification for {} has already been closed.",
                        leaderElectionDriver);
                return;
            }

            issuedLeaderSessionID = UUID.randomUUID();
            clearConfirmedLeaderInformation();

            LOG.info(
                    "Grant leadership to contender {} with session ID {}.",
                    leaderContender.getDescription(),
                    issuedLeaderSessionID);
            leaderContender.grantLeadership(issuedLeaderSessionID);
        }
    }

    @Override
    public void onRevokeLeadership() {
        synchronized (lock) {
            if (!running) {
                LOG.warn(
                        "Ignoring the revoke leadership notification since {} has already been closed.",
                        leaderElectionDriver);
                return;
            }

            LOG.info(
                    "Revoke leadership of {}-{}.",
                    leaderContender.getDescription(),
                    confirmedLeaderInfo);

            issuedLeaderSessionID = null;
            clearConfirmedLeaderInformation();
            leaderContender.revokeLeadership();

            LOG.info("Clearing the leader information on {}.", leaderElectionDriver);
            // Clear the old leader information on the external storage
            leaderElectionDriver.writeLeaderInformation(LeaderInformation.empty());
        }
    }

    @Override
    public void onLeaderInformationChange(LeaderInformation leaderInfo) {
        synchronized (lock) {
            if (!running) {
                LOG.warn(
                        "Ignoring change notification since the {} has already been closed.",
                        leaderElectionDriver);
                return;
            }

            LOG.info(
                    "Leader node changed while {} is the leader. Old leader information {}; New "
                            + "leader information {}.",
                    leaderContender.getDescription(),
                    confirmedLeaderInfo,
                    leaderInfo);

            if (confirmedLeaderInfo == null) {
                return;
            }

            if (leaderInfo.isEmpty() || !leaderInfo.equals(confirmedLeaderInfo)) {
                LOG.info(
                        "Writing leader information {} of {}.",
                        confirmedLeaderInfo,
                        leaderContender.getDescription());
                leaderElectionDriver.writeLeaderInformation(confirmedLeaderInfo);
            }
        }
    }

    private class LeaderElectionFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable throwable) {
            synchronized (lock) {
                if (!running) {
                    LOG.debug("Ignoring error notification since the service has been stopped.");
                    return;
                }
                leaderContender.handleError(throwable);
            }
        }
    }
}
