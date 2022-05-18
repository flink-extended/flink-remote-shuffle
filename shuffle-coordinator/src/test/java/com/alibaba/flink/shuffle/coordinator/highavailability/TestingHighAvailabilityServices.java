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

/**
 * A variant of the HighAvailabilityServices for testing. Each individual service can be set to an
 * arbitrary implementation, such as a mock or default service.
 */
public class TestingHighAvailabilityServices implements HaServices {

    private LeaderRetrievalService shuffleManagerLeaderRetrievalService;

    private LeaderElectionService shuffleManagerLeaderElectionService;

    public void setShuffleManagerLeaderRetrievalService(
            LeaderRetrievalService shuffleManagerLeaderRetrievalService) {
        this.shuffleManagerLeaderRetrievalService = shuffleManagerLeaderRetrievalService;
    }

    public void setShuffleManagerLeaderElectionService(
            LeaderElectionService shuffleManagerLeaderElectionService) {
        this.shuffleManagerLeaderElectionService = shuffleManagerLeaderElectionService;
    }

    // ------------------------------------------------------------------------
    //  Shutdown
    // ------------------------------------------------------------------------

    @Override
    public void close() throws Exception {
        // nothing to do
    }

    @Override
    public LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor) {
        return shuffleManagerLeaderRetrievalService;
    }

    @Override
    public LeaderElectionService createLeaderElectionService() {
        return shuffleManagerLeaderElectionService;
    }

    @Override
    public void closeAndCleanupAllData() throws Exception {
        // nothing to do
    }
}
