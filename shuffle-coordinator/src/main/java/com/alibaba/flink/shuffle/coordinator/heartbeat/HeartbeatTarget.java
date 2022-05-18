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
 * Interface for components which can be sent heartbeats and from which one can request a heartbeat
 * response. Both the heartbeat response and the heartbeat request can carry a payload. This payload
 * is reported to the heartbeat target and contains additional information. The payload can be empty
 * which is indicated by a null value.
 *
 * @param <I> Type of the payload which is sent to the heartbeat target
 */
public interface HeartbeatTarget<I> {

    /**
     * Sends a heartbeat response to the target. Each heartbeat response can carry a payload which
     * contains additional information for the heartbeat target.
     *
     * @param heartbeatOrigin Resource ID identifying the machine for which a heartbeat shall be
     *     reported.
     * @param heartbeatPayload Payload of the heartbeat. Null indicates an empty payload.
     */
    void receiveHeartbeat(InstanceID heartbeatOrigin, I heartbeatPayload);

    /**
     * Requests a heartbeat from the target. Each heartbeat request can carry a payload which
     * contains additional information for the heartbeat target.
     *
     * @param requestOrigin Resource ID identifying the machine issuing the heartbeat request.
     * @param heartbeatPayload Payload of the heartbeat request. Null indicates an empty payload.
     */
    void requestHeartbeat(InstanceID requestOrigin, I heartbeatPayload);
}
