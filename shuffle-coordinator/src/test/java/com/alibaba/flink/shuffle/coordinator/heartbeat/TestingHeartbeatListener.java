/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.function.Consumer;
import java.util.function.Function;

/** {@link HeartbeatListener} implementation for tests. */
final class TestingHeartbeatListener<I, O> implements HeartbeatListener<I, O> {

    private final Consumer<InstanceID> notifyHeartbeatTimeoutConsumer;

    private final BiConsumer<InstanceID, I> reportPayloadConsumer;

    private final Function<InstanceID, O> retrievePayloadFunction;

    TestingHeartbeatListener(
            Consumer<InstanceID> notifyHeartbeatTimeoutConsumer,
            BiConsumer<InstanceID, I> reportPayloadConsumer,
            Function<InstanceID, O> retrievePayloadFunction) {
        this.notifyHeartbeatTimeoutConsumer = notifyHeartbeatTimeoutConsumer;
        this.reportPayloadConsumer = reportPayloadConsumer;
        this.retrievePayloadFunction = retrievePayloadFunction;
    }

    @Override
    public void notifyHeartbeatTimeout(InstanceID instanceID) {
        notifyHeartbeatTimeoutConsumer.accept(instanceID);
    }

    @Override
    public void reportPayload(InstanceID instanceID, I payload) {
        reportPayloadConsumer.accept(instanceID, payload);
    }

    @Override
    public O retrievePayload(InstanceID instanceID) {
        return retrievePayloadFunction.apply(instanceID);
    }
}
