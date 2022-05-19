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

import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.leaderelection.TestingListener;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

/** Tests for {@link SettableLeaderRetrievalService}. */
public class SettableLeaderRetrievalServiceTest extends TestLogger {

    private SettableLeaderRetrievalService settableLeaderRetrievalService;

    @Before
    public void setup() {
        settableLeaderRetrievalService = new SettableLeaderRetrievalService();
    }

    @Test
    public void testNotifyListenerLater() throws Exception {
        final String localhost = "localhost";
        settableLeaderRetrievalService.notifyListener(
                new LeaderInformation(HaServices.DEFAULT_LEADER_ID, localhost));

        final TestingListener listener = new TestingListener();
        settableLeaderRetrievalService.start(listener);

        assertThat(listener.getAddress(), equalTo(localhost));
        assertThat(listener.getLeaderSessionID(), equalTo(HaServices.DEFAULT_LEADER_ID));
    }

    @Test
    public void testNotifyListenerImmediately() throws Exception {
        final TestingListener listener = new TestingListener();
        settableLeaderRetrievalService.start(listener);

        final String localhost = "localhost";
        settableLeaderRetrievalService.notifyListener(
                new LeaderInformation(HaServices.DEFAULT_LEADER_ID, localhost));

        assertThat(listener.getAddress(), equalTo(localhost));
        assertThat(listener.getLeaderSessionID(), equalTo(HaServices.DEFAULT_LEADER_ID));
    }
}
