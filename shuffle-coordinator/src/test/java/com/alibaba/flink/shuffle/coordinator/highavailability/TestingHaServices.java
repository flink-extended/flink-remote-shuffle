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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Testing {@link HaServices} implementation. */
public class TestingHaServices implements HaServices {

    private LeaderRetrievalService shuffleManagerLeaderRetrieveService;

    private LeaderElectionService shuffleManagerLeaderElectionService;

    // ------------------------------------------------------------------------
    //  Setters for mock / testing implementations
    // ------------------------------------------------------------------------

    public void setShuffleManagerLeaderRetrieveService(
            LeaderRetrievalService shuffleManagerLeaderRetrieveService) {
        this.shuffleManagerLeaderRetrieveService = shuffleManagerLeaderRetrieveService;
    }

    public void setShuffleManagerLeaderElectionService(
            LeaderElectionService shuffleManagerLeaderElectionService) {
        this.shuffleManagerLeaderElectionService = shuffleManagerLeaderElectionService;
    }

    // ------------------------------------------------------------------------
    //  HA Services Methods
    // ------------------------------------------------------------------------

    @Override
    public LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor) {
        checkNotNull(shuffleManagerLeaderRetrieveService);

        return shuffleManagerLeaderRetrieveService;
    }

    @Override
    public LeaderElectionService createLeaderElectionService() {
        checkNotNull(shuffleManagerLeaderElectionService);

        return shuffleManagerLeaderElectionService;
    }

    @Override
    public void closeAndCleanupAllData() throws Exception {}

    @Override
    public void close() throws Exception {}
}
