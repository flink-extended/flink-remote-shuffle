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
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;

import javax.annotation.concurrent.GuardedBy;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * An implementation of the {@link HaServices} for the non-high-availability case. This
 * implementation can be used for testing, and for cluster setups that do not tolerate failures of
 * the master processes.
 *
 * <p>This implementation has no dependencies on any external services. It returns a fix
 * pre-configured ShuffleManager, and stores checkpoints and metadata simply on the heap or on a
 * local file system and therefore in a storage without guarantees.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices).
 */
public class StandaloneHaServices implements HaServices {

    protected final Object lock = new Object();

    /** The fix address of the ShuffleManager. */
    private final String shuffleManagerAddress;

    private boolean shutdown;

    public StandaloneHaServices(String shuffleManagerAddress) {
        this.shuffleManagerAddress = checkNotNull(shuffleManagerAddress);
    }

    @Override
    public LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor) {
        synchronized (lock) {
            checkNotShutdown();

            return new StandaloneLeaderRetrievalService(
                    new LeaderInformation(DEFAULT_LEADER_ID, shuffleManagerAddress));
        }
    }

    @Override
    public LeaderElectionService createLeaderElectionService() {
        synchronized (lock) {
            checkNotShutdown();

            return new StandaloneLeaderElectionService();
        }
    }

    @GuardedBy("lock")
    protected void checkNotShutdown() {
        checkState(!shutdown, "high availability services are shut down");
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
            }
        }
    }

    @Override
    public void closeAndCleanupAllData() {
        this.close();
    }
}
