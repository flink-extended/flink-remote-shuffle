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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatListener;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManager;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManagerImpl;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManagerSenderImpl;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatTarget;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/** Special {@link HeartbeatServices} which records the unmonitored targets. */
public class RecordingHeartbeatServices extends HeartbeatServices {

    private final BlockingQueue<InstanceID> unmonitoredTargets;

    private final BlockingQueue<InstanceID> monitoredTargets;

    public RecordingHeartbeatServices(long heartbeatInterval, long heartbeatTimeout) {
        super(heartbeatInterval, heartbeatTimeout);

        this.unmonitoredTargets = new ArrayBlockingQueue<>(1);
        this.monitoredTargets = new ArrayBlockingQueue<>(1);
    }

    @Override
    public <I, O> HeartbeatManager<I, O> createHeartbeatManager(
            InstanceID instanceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {
        return new RecordingHeartbeatManagerImpl<>(
                heartbeatTimeout,
                instanceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                unmonitoredTargets,
                monitoredTargets);
    }

    @Override
    public <I, O> HeartbeatManager<I, O> createHeartbeatManagerSender(
            InstanceID instanceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {
        return new RecordingHeartbeatManagerSenderImpl<>(
                heartbeatInterval,
                heartbeatTimeout,
                instanceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                unmonitoredTargets,
                monitoredTargets);
    }

    public BlockingQueue<InstanceID> getUnmonitoredTargets() {
        return unmonitoredTargets;
    }

    public BlockingQueue<InstanceID> getMonitoredTargets() {
        return monitoredTargets;
    }

    /** {@link HeartbeatManagerImpl} which records the unmonitored targets. */
    private static final class RecordingHeartbeatManagerImpl<I, O>
            extends HeartbeatManagerImpl<I, O> {

        private final BlockingQueue<InstanceID> unmonitoredTargets;

        private final BlockingQueue<InstanceID> monitoredTargets;

        public RecordingHeartbeatManagerImpl(
                long heartbeatTimeoutIntervalMs,
                InstanceID ownInstanceID,
                HeartbeatListener<I, O> heartbeatListener,
                ScheduledExecutor mainThreadExecutor,
                Logger log,
                BlockingQueue<InstanceID> unmonitoredTargets,
                BlockingQueue<InstanceID> monitoredTargets) {
            super(
                    heartbeatTimeoutIntervalMs,
                    ownInstanceID,
                    heartbeatListener,
                    mainThreadExecutor,
                    log);
            this.unmonitoredTargets = unmonitoredTargets;
            this.monitoredTargets = monitoredTargets;
        }

        @Override
        public void unmonitorTarget(InstanceID instanceID) {
            super.unmonitorTarget(instanceID);
            unmonitoredTargets.offer(instanceID);
        }

        @Override
        public void monitorTarget(InstanceID instanceID, HeartbeatTarget<O> heartbeatTarget) {
            super.monitorTarget(instanceID, heartbeatTarget);
            monitoredTargets.offer(instanceID);
        }
    }

    /** {@link HeartbeatManagerSenderImpl} which records the unmonitored targets. */
    private static final class RecordingHeartbeatManagerSenderImpl<I, O>
            extends HeartbeatManagerSenderImpl<I, O> {

        private final BlockingQueue<InstanceID> unmonitoredTargets;

        private final BlockingQueue<InstanceID> monitoredTargets;

        public RecordingHeartbeatManagerSenderImpl(
                long heartbeatPeriod,
                long heartbeatTimeout,
                InstanceID ownInstanceID,
                HeartbeatListener<I, O> heartbeatListener,
                ScheduledExecutor mainThreadExecutor,
                Logger log,
                BlockingQueue<InstanceID> unmonitoredTargets,
                BlockingQueue<InstanceID> monitoredTargets) {
            super(
                    heartbeatPeriod,
                    heartbeatTimeout,
                    ownInstanceID,
                    heartbeatListener,
                    mainThreadExecutor,
                    log);
            this.unmonitoredTargets = unmonitoredTargets;
            this.monitoredTargets = monitoredTargets;
        }

        @Override
        public void unmonitorTarget(InstanceID instanceID) {
            super.unmonitorTarget(instanceID);
            unmonitoredTargets.offer(instanceID);
        }

        @Override
        public void monitorTarget(InstanceID instanceID, HeartbeatTarget<O> heartbeatTarget) {
            super.monitorTarget(instanceID, heartbeatTarget);
            monitoredTargets.offer(instanceID);
        }
    }
}
