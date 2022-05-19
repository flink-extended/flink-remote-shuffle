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

package com.alibaba.flink.shuffle.coordinator.highavailability.standalone;

import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderContender;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;

import javax.annotation.Nonnull;

import java.util.UUID;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * A Standalone implementation of the {@link LeaderElectionService} interface. The standalone
 * implementation assumes that there is only a single {@link LeaderContender} and thus directly
 * grants him the leadership upon start up. Furthermore, there is no communication needed between
 * multiple standalone leader election services.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService).
 */
public class StandaloneLeaderElectionService implements LeaderElectionService {

    private LeaderContender contender = null;

    @Override
    public void start(LeaderContender newContender) throws Exception {
        if (contender != null) {
            // Service was already started
            throw new IllegalArgumentException(
                    "Leader election service cannot be started multiple times.");
        }

        contender = checkNotNull(newContender);

        // directly grant leadership to the given contender
        contender.grantLeadership(HaServices.DEFAULT_LEADER_ID);
    }

    @Override
    public void stop() {
        if (contender != null) {
            contender.revokeLeadership();
            contender = null;
        }
    }

    @Override
    public void confirmLeadership(LeaderInformation leaderInfo) {}

    @Override
    public boolean hasLeadership(@Nonnull UUID leaderSessionId) {
        return (contender != null && HaServices.DEFAULT_LEADER_ID.equals(leaderSessionId));
    }
}
