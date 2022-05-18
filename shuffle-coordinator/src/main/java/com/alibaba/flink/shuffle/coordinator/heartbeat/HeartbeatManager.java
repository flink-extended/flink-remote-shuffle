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

package com.alibaba.flink.shuffle.coordinator.heartbeat;

import com.alibaba.flink.shuffle.core.ids.InstanceID;

/**
 * A heartbeat manager has to be able to start/stop monitoring a {@link HeartbeatTarget}, and report
 * heartbeat timeouts for this target.
 *
 * @param <I> Type of the incoming payload
 * @param <O> Type of the outgoing payload
 */
public interface HeartbeatManager<I, O> extends HeartbeatTarget<I> {

    /**
     * Start monitoring a {@link HeartbeatTarget}. Heartbeat timeouts for this target are reported
     * to the {@link HeartbeatListener} associated with this heartbeat manager.
     *
     * @param instanceID Resource ID identifying the heartbeat target
     * @param heartbeatTarget Interface to send heartbeat requests and responses to the heartbeat
     *     target
     */
    void monitorTarget(InstanceID instanceID, HeartbeatTarget<O> heartbeatTarget);

    /**
     * Stops monitoring the heartbeat target with the associated resource ID.
     *
     * @param instanceID Resource ID of the heartbeat target which shall no longer be monitored
     */
    void unmonitorTarget(InstanceID instanceID);

    /** Stops the heartbeat manager. */
    void stop();

    /**
     * Returns the last received heartbeat from the given target.
     *
     * @param instanceID for which to return the last heartbeat
     * @return Last heartbeat received from the given target or -1 if the target is not being
     *     monitored.
     */
    long getLastHeartbeatFrom(InstanceID instanceID);
}
