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

package com.alibaba.flink.shuffle.coordinator.leaderretrieval;

import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriver;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalDriverFactory;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalEventHandler;

import javax.annotation.Nullable;

/**
 * {@link LeaderRetrievalDriver} implementation which provides some convenience functions for
 * testing purposes.
 */
public class TestingLeaderRetrievalDriver implements LeaderRetrievalDriver {

    private final LeaderRetrievalEventHandler leaderRetrievalEventHandler;
    private final FatalErrorHandler fatalErrorHandler;

    private TestingLeaderRetrievalDriver(
            LeaderRetrievalEventHandler leaderRetrievalEventHandler,
            FatalErrorHandler fatalErrorHandler) {
        this.leaderRetrievalEventHandler = leaderRetrievalEventHandler;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public void close() throws Exception {
        // noop
    }

    public void onUpdate(LeaderInformation newLeader) {
        leaderRetrievalEventHandler.notifyLeaderAddress(newLeader);
    }

    public void onFatalError(Throwable throwable) {
        fatalErrorHandler.onFatalError(throwable);
    }

    /** Factory for create {@link TestingLeaderRetrievalDriver}. */
    public static class TestingLeaderRetrievalDriverFactory
            implements LeaderRetrievalDriverFactory {

        private TestingLeaderRetrievalDriver currentDriver;

        @Override
        public LeaderRetrievalDriver createLeaderRetrievalDriver(
                LeaderRetrievalEventHandler leaderEventHandler,
                FatalErrorHandler fatalErrorHandler) {
            currentDriver = new TestingLeaderRetrievalDriver(leaderEventHandler, fatalErrorHandler);
            return currentDriver;
        }

        @Nullable
        public TestingLeaderRetrievalDriver getCurrentRetrievalDriver() {
            return currentDriver;
        }
    }
}
