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

import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderContender;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionEventHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.utils.CommonTestUtils;

import javax.annotation.Nullable;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Base class which provides some convenience functions for testing purposes of {@link
 * LeaderContender} and {@link LeaderElectionEventHandler}.
 */
public class TestingLeaderBase {
    // The queues will be offered by subclasses
    protected final BlockingQueue<LeaderInformation> leaderEventQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    private boolean isLeader = false;
    private Throwable error;

    public void waitForLeader(long timeout) throws Exception {
        throwExceptionIfNotNull();

        final String errorMsg = "Contender was not elected as the leader within " + timeout + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final LeaderInformation leader =
                            leaderEventQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    return leader != null && !leader.isEmpty();
                },
                timeout,
                errorMsg);

        isLeader = true;
    }

    public void waitForRevokeLeader(long timeout) throws Exception {
        throwExceptionIfNotNull();

        final String errorMsg = "Contender was not revoked within " + timeout + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final LeaderInformation leader =
                            leaderEventQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    return leader != null && leader.isEmpty();
                },
                timeout,
                errorMsg);

        isLeader = false;
    }

    public void waitForError(long timeout) throws Exception {
        final String errorMsg = "Contender did not see an exception with " + timeout + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> {
                    error = errorQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    return error != null;
                },
                timeout,
                errorMsg);
    }

    public void handleError(Throwable ex) {
        errorQueue.offer(ex);
    }

    /**
     * Please use {@link #waitForError} before get the error.
     *
     * @return the error has been handled.
     */
    @Nullable
    public Throwable getError() {
        return this.error;
    }

    public boolean isLeader() {
        return isLeader;
    }

    private void throwExceptionIfNotNull() throws Exception {
        if (error != null) {
            ExceptionUtils.rethrowException(error);
        }
    }
}
