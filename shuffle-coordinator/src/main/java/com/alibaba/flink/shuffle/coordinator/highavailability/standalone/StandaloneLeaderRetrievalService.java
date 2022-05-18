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

package com.alibaba.flink.shuffle.coordinator.highavailability.standalone;

import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalListener;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A Standalone implementation of the {@link LeaderRetrievalService}. This implementation assumes
 * that there is only a single contender for leadership (e.g., a single ShuffleManager process) and
 * that this process is reachable under a constant address.
 *
 * <p>As soon as this service is started, it immediately notifies the leader listener of the leader
 * contender with the pre-configured address.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService).
 */
public class StandaloneLeaderRetrievalService implements LeaderRetrievalService {

    private final Object startStopLock = new Object();

    /** Leader information including leader address and leader ID. */
    private final LeaderInformation leaderInfo;

    /** Flag whether this service is started. */
    private boolean started;

    /** Creates a StandaloneLeaderRetrievalService with the given leader address. */
    public StandaloneLeaderRetrievalService(LeaderInformation leaderInfo) {
        this.leaderInfo = checkNotNull(leaderInfo);
    }

    // ------------------------------------------------------------------------

    @Override
    public void start(LeaderRetrievalListener listener) {
        checkArgument(listener != null, "Listener must not be null.");

        synchronized (startStopLock) {
            checkState(!started, "StandaloneLeaderRetrievalService can only be started once.");
            started = true;

            // directly notify the listener, because we already know the leading ShuffleManager's
            // address
            listener.notifyLeaderAddress(leaderInfo);
        }
    }

    @Override
    public void stop() {
        synchronized (startStopLock) {
            started = false;
        }
    }
}
