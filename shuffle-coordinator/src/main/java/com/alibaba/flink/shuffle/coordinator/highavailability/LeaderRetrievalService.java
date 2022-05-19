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

package com.alibaba.flink.shuffle.coordinator.highavailability;

/**
 * This interface has to be implemented by a service which retrieves the current leader and notifies
 * a listener about it.
 *
 * <p>Prior to using this service it has to be started by calling the start method. The start method
 * also takes the {@link LeaderRetrievalListener} as an argument. The service can only be started
 * once.
 *
 * <p>The service should be stopped by calling the stop method.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService).
 */
public interface LeaderRetrievalService {

    /**
     * Starts the leader retrieval service with the given listener to listen for new leaders. This
     * method can only be called once.
     *
     * @param listener The leader retrieval listener which will be notified about new leaders.
     */
    void start(LeaderRetrievalListener listener) throws Exception;

    /** Stops the leader retrieval service. */
    void stop() throws Exception;
}
