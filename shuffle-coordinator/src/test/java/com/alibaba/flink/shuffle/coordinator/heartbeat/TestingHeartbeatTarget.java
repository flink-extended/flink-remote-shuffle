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

import java.util.function.BiConsumer;

/** {@link HeartbeatTarget} implementation for tests. */
class TestingHeartbeatTarget<T> implements HeartbeatTarget<T> {
    private final BiConsumer<InstanceID, T> receiveHeartbeatConsumer;

    private final BiConsumer<InstanceID, T> requestHeartbeatConsumer;

    TestingHeartbeatTarget(
            BiConsumer<InstanceID, T> receiveHeartbeatConsumer,
            BiConsumer<InstanceID, T> requestHeartbeatConsumer) {
        this.receiveHeartbeatConsumer = receiveHeartbeatConsumer;
        this.requestHeartbeatConsumer = requestHeartbeatConsumer;
    }

    @Override
    public void receiveHeartbeat(InstanceID heartbeatOrigin, T heartbeatPayload) {
        receiveHeartbeatConsumer.accept(heartbeatOrigin, heartbeatPayload);
    }

    @Override
    public void requestHeartbeat(InstanceID requestOrigin, T heartbeatPayload) {
        requestHeartbeatConsumer.accept(requestOrigin, heartbeatPayload);
    }
}
