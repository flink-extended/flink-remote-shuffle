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

import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.standalone.StandaloneLeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.standalone.StandaloneLeaderRetrievalService;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link StandaloneLeaderElectionService}. */
public class StandaloneLeaderElectionTest extends TestLogger {

    private static final String TEST_URL = "akka://users/shufflemanager";

    /**
     * Tests that the standalone leader election and retrieval service return the same leader URL.
     */
    @Test
    public void testStandaloneLeaderElectionRetrieval() throws Exception {
        StandaloneLeaderElectionService leaderElectionService =
                new StandaloneLeaderElectionService();
        StandaloneLeaderRetrievalService leaderRetrievalService =
                new StandaloneLeaderRetrievalService(
                        new LeaderInformation(HaServices.DEFAULT_LEADER_ID, TEST_URL));
        TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
        TestingListener testingListener = new TestingListener();

        try {
            leaderElectionService.start(contender);
            leaderRetrievalService.start(testingListener);

            contender.waitForLeader(1000L);

            assertTrue(contender.isLeader());
            assertEquals(HaServices.DEFAULT_LEADER_ID, contender.getLeaderSessionID());

            testingListener.waitForNewLeader(1000L);

            assertEquals(TEST_URL, testingListener.getAddress());
            assertEquals(HaServices.DEFAULT_LEADER_ID, testingListener.getLeaderSessionID());
        } finally {
            leaderElectionService.stop();
            leaderRetrievalService.stop();
        }
    }
}
