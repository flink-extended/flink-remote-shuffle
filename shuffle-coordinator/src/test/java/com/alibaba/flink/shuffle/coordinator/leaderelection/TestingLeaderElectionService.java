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

package com.alibaba.flink.shuffle.coordinator.leaderelection;

import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderContender;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.utils.LeaderConnectionInfo;

import javax.annotation.Nonnull;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Test {@link LeaderElectionService} implementation which directly forwards isLeader and notLeader
 * calls to the contender.
 */
public class TestingLeaderElectionService implements LeaderElectionService {

    private LeaderContender contender = null;
    private boolean hasLeadership = false;
    private CompletableFuture<LeaderConnectionInfo> confirmationFuture = null;
    private CompletableFuture<Void> startFuture = new CompletableFuture<>();
    private UUID issuedLeaderSessionId = null;

    /**
     * Gets a future that completes when leadership is confirmed.
     *
     * <p>Note: the future is created upon calling {@link #isLeader(UUID)}.
     */
    public synchronized CompletableFuture<LeaderConnectionInfo> getConfirmationFuture() {
        return confirmationFuture;
    }

    @Override
    public synchronized void start(LeaderContender contender) {
        assert (!getStartFuture().isDone());

        this.contender = contender;

        if (hasLeadership) {
            contender.grantLeadership(issuedLeaderSessionId);
        }

        startFuture.complete(null);
    }

    @Override
    public synchronized void stop() throws Exception {
        contender = null;
        hasLeadership = false;
        issuedLeaderSessionId = null;
        startFuture.cancel(false);
        startFuture = new CompletableFuture<>();
    }

    @Override
    public synchronized void confirmLeadership(LeaderInformation leaderInfo) {
        if (confirmationFuture != null) {
            confirmationFuture.complete(
                    new LeaderConnectionInfo(
                            leaderInfo.getLeaderSessionID(), leaderInfo.getLeaderAddress()));
        }
    }

    @Override
    public synchronized boolean hasLeadership(@Nonnull UUID leaderSessionId) {
        return hasLeadership && leaderSessionId.equals(issuedLeaderSessionId);
    }

    public synchronized CompletableFuture<UUID> isLeader(UUID leaderSessionID) {
        if (confirmationFuture != null) {
            confirmationFuture.cancel(false);
        }
        confirmationFuture = new CompletableFuture<>();
        hasLeadership = true;
        issuedLeaderSessionId = leaderSessionID;

        if (contender != null) {
            contender.grantLeadership(leaderSessionID);
        }

        return confirmationFuture.thenApply(LeaderConnectionInfo::getLeaderSessionId);
    }

    public synchronized void notLeader() {
        hasLeadership = false;

        if (contender != null) {
            contender.revokeLeadership();
        }
    }

    public synchronized String getAddress() {
        if (confirmationFuture.isDone()) {
            return confirmationFuture.join().getAddress();
        } else {
            throw new IllegalStateException("TestingLeaderElectionService has not been started.");
        }
    }

    /**
     * Returns the start future indicating whether this leader election service has been started or
     * not.
     *
     * @return Future which is completed once this service has been started
     */
    public synchronized CompletableFuture<Void> getStartFuture() {
        return startFuture;
    }
}
