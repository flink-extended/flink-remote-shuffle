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

import java.util.UUID;

/**
 * The HighAvailabilityServices gives access to all services needed for a highly-available setup.
 */
public interface HaServices extends AutoCloseable {

    // ------------------------------------------------------------------------
    //  Constants
    // ------------------------------------------------------------------------

    /**
     * This UUID should be used when no proper leader election happens, but a simple pre-configured
     * leader is used. That is for example the case in non-highly-available standalone setups.
     */
    UUID DEFAULT_LEADER_ID = new UUID(0, 0);

    /** Creates the shuffle manager leader retriever for the shuffle worker and shuffle client. */
    LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor);

    /**
     * Creates the leader election service for the shuffle manager.
     *
     * @return Leader election service for the shuffle manager election.
     */
    LeaderElectionService createLeaderElectionService();

    /**
     * Closes the high availability services (releasing all resources) and deletes all data stored
     * by these services in external stores.
     *
     * <p>If an exception occurs during cleanup, this method will attempt to continue the cleanup
     * and report exceptions only after all cleanup steps have been attempted.
     *
     * @throws Exception Thrown, if an exception occurred while closing these services or cleaning
     *     up data stored by them.
     */
    void closeAndCleanupAllData() throws Exception;

    /**
     * Type of leader information receptor which will retrieve the {@link LeaderInformation} of
     * shuffle manager.
     */
    enum LeaderReceptor {
        SHUFFLE_CLIENT,
        SHUFFLE_WORKER
    }
}
