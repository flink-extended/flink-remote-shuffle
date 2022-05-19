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

import java.util.UUID;

/**
 * Interface for a service which allows to elect a leader among a group of contenders.
 *
 * <p>Prior to using this service, it has to be started calling the start method. The start method
 * takes the contender as a parameter. If there are multiple contenders, then each contender has to
 * instantiate its own leader election service.
 *
 * <p>Once a contender has been granted leadership he has to confirm the received leader session ID
 * by calling the method {@link #confirmLeadership(LeaderInformation)} ()}. This will notify the
 * leader election service, that the contender has accepted the leadership specified and that the
 * leader session id as well as the leader address can now be published for leader retrieval
 * services.
 *
 * <p>This class is copied and modified from Apache Flink
 * (org.apache.flink.runtime.leaderelection.LeaderElectionService).
 */
public interface LeaderElectionService {

    /**
     * Starts the leader election service. This method can only be called once.
     *
     * @param contender LeaderContender which applies for the leadership.
     */
    void start(LeaderContender contender) throws Exception;

    /** Stops the leader election service. */
    void stop() throws Exception;

    /**
     * Confirms that the {@link LeaderContender} has accepted the leadership identified by the given
     * leader session id. It also publishes the leader address under which the leader is reachable.
     *
     * <p>The rational behind this method is to establish an order between setting the new leader
     * session ID in the {@link LeaderContender} and publishing the new leader session ID as well as
     * the leader address to the leader retrieval services.
     */
    void confirmLeadership(LeaderInformation leaderInfo);

    /**
     * Returns true if the {@link LeaderContender} with which the service has been started owns
     * currently the leadership under the given leader session id.
     *
     * @param leaderSessionId identifying the current leader
     * @return true if the associated {@link LeaderContender} is the leader, otherwise false
     */
    boolean hasLeadership(UUID leaderSessionId);
}
