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
 * A {@link HeartbeatManager} implementation which does nothing.
 *
 * @param <I> ignored
 * @param <O> ignored
 */
public class NoOpHeartbeatManager<I, O> implements HeartbeatManager<I, O> {
    private static final NoOpHeartbeatManager<Object, Object> INSTANCE =
            new NoOpHeartbeatManager<>();

    private NoOpHeartbeatManager() {}

    @Override
    public void monitorTarget(InstanceID instanceID, HeartbeatTarget<O> heartbeatTarget) {}

    @Override
    public void unmonitorTarget(InstanceID instanceID) {}

    @Override
    public void stop() {}

    @Override
    public long getLastHeartbeatFrom(InstanceID instanceID) {
        return 0;
    }

    @Override
    public void receiveHeartbeat(InstanceID heartbeatOrigin, I heartbeatPayload) {}

    @Override
    public void requestHeartbeat(InstanceID requestOrigin, I heartbeatPayload) {}

    @SuppressWarnings("unchecked")
    public static <A, B> NoOpHeartbeatManager<A, B> getInstance() {
        return (NoOpHeartbeatManager<A, B>) INSTANCE;
    }
}
