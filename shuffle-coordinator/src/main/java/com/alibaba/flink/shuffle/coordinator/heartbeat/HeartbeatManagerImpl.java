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

package com.alibaba.flink.shuffle.coordinator.heartbeat;

import com.alibaba.flink.shuffle.core.ids.InstanceID;

import org.slf4j.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * Heartbeat manager implementation. The heartbeat manager maintains a map of heartbeat monitors and
 * resource IDs. Each monitor will be updated when a new heartbeat of the associated machine has
 * been received. If the monitor detects that a heartbeat has timed out, it will notify the {@link
 * HeartbeatListener} about it. A heartbeat times out iff no heartbeat signal has been received
 * within a given timeout interval.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
@ThreadSafe
public class HeartbeatManagerImpl<I, O> implements HeartbeatManager<I, O> {

    /** Heartbeat timeout interval in milli seconds. */
    private final long heartbeatTimeoutIntervalMs;

    /** Resource ID which is used to mark one own's heartbeat signals. */
    private final InstanceID ownInstanceID;

    /** Heartbeat listener with which the heartbeat manager has been associated. */
    private final HeartbeatListener<I, O> heartbeatListener;

    /** Executor service used to run heartbeat timeout notifications. */
    private final ScheduledExecutorService scheduledExecutor;

    protected final Logger log;

    /** Map containing the heartbeat monitors associated with the respective resource ID. */
    private final ConcurrentHashMap<InstanceID, HeartbeatMonitor<O>> heartbeatTargets;

    private final HeartbeatMonitor.Factory<O> heartbeatMonitorFactory;

    /** Running state of the heartbeat manager. */
    protected volatile boolean stopped;

    public HeartbeatManagerImpl(
            long heartbeatTimeoutIntervalMs,
            InstanceID ownInstanceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutorService scheduledExecutor,
            Logger log) {
        this(
                heartbeatTimeoutIntervalMs,
                ownInstanceID,
                heartbeatListener,
                scheduledExecutor,
                log,
                new HeartbeatMonitorImpl.Factory<>());
    }

    public HeartbeatManagerImpl(
            long heartbeatTimeoutIntervalMs,
            InstanceID ownInstanceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutorService scheduledExecutor,
            Logger log,
            HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {

        checkArgument(
                heartbeatTimeoutIntervalMs > 0L, "The heartbeat timeout has to be larger than 0.");

        this.heartbeatTimeoutIntervalMs = heartbeatTimeoutIntervalMs;
        this.ownInstanceID = checkNotNull(ownInstanceID);
        this.heartbeatListener = checkNotNull(heartbeatListener);
        this.scheduledExecutor = checkNotNull(scheduledExecutor);
        this.log = checkNotNull(log);
        this.heartbeatMonitorFactory = heartbeatMonitorFactory;
        this.heartbeatTargets = new ConcurrentHashMap<>(16);
    }

    // ----------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------

    InstanceID getOwnInstanceID() {
        return ownInstanceID;
    }

    HeartbeatListener<I, O> getHeartbeatListener() {
        return heartbeatListener;
    }

    public Map<InstanceID, HeartbeatMonitor<O>> getHeartbeatTargets() {
        return heartbeatTargets;
    }

    // ----------------------------------------------------------------------------------------------
    // HeartbeatManager methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public void monitorTarget(InstanceID instanceID, HeartbeatTarget<O> heartbeatTarget) {
        if (!stopped) {
            // always monitor the new target to avoid the old monitor not in running state
            HeartbeatMonitor<O> heartbeatMonitor =
                    heartbeatMonitorFactory.createHeartbeatMonitor(
                            instanceID,
                            heartbeatTarget,
                            scheduledExecutor,
                            heartbeatListener,
                            heartbeatTimeoutIntervalMs,
                            log);

            HeartbeatMonitor<O> oldMonitor = heartbeatTargets.put(instanceID, heartbeatMonitor);
            if (oldMonitor != null) {
                oldMonitor.cancel();
                log.debug(
                        "Replace the heartbeat monitor of target (instance id: {}) with new one.",
                        instanceID);
            }

            // check if we have stopped in the meantime (concurrent stop operation)
            if (stopped) {
                heartbeatMonitor.cancel();

                heartbeatTargets.remove(instanceID);
            }
        }
    }

    @Override
    public void unmonitorTarget(InstanceID instanceID) {
        if (!stopped) {
            HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.remove(instanceID);

            if (heartbeatMonitor != null) {
                heartbeatMonitor.cancel();
            }
        }
    }

    @Override
    public void stop() {
        stopped = true;

        for (HeartbeatMonitor<O> heartbeatMonitor : heartbeatTargets.values()) {
            try {
                heartbeatMonitor.cancel();
            } catch (Throwable throwable) {
                log.warn("Failed to cancel heartbeat.", throwable);
            }
        }

        heartbeatTargets.clear();
        scheduledExecutor.shutdown();
    }

    @Override
    public long getLastHeartbeatFrom(InstanceID instanceID) {
        HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(instanceID);

        if (heartbeatMonitor != null) {
            return heartbeatMonitor.getLastHeartbeat();
        } else {
            return -1L;
        }
    }

    ScheduledExecutorService getScheduledExecutor() {
        return scheduledExecutor;
    }

    // ----------------------------------------------------------------------------------------------
    // HeartbeatTarget methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public void receiveHeartbeat(InstanceID heartbeatOrigin, I heartbeatPayload) {
        if (!stopped) {
            log.debug("Received heartbeat from {}.", heartbeatOrigin);
            reportHeartbeat(heartbeatOrigin);

            if (heartbeatPayload != null) {
                heartbeatListener.reportPayload(heartbeatOrigin, heartbeatPayload);
            }
        }
    }

    @Override
    public void requestHeartbeat(final InstanceID requestOrigin, I heartbeatPayload) {
        if (!stopped) {
            log.debug("Received heartbeat request from {}.", requestOrigin);

            final HeartbeatTarget<O> heartbeatTarget = reportHeartbeat(requestOrigin);

            if (heartbeatTarget != null) {
                if (heartbeatPayload != null) {
                    heartbeatListener.reportPayload(requestOrigin, heartbeatPayload);
                }

                heartbeatTarget.receiveHeartbeat(
                        getOwnInstanceID(), heartbeatListener.retrievePayload(requestOrigin));
            }
        }
    }

    HeartbeatTarget<O> reportHeartbeat(InstanceID instanceID) {
        if (heartbeatTargets.containsKey(instanceID)) {
            HeartbeatMonitor<O> heartbeatMonitor = heartbeatTargets.get(instanceID);
            heartbeatMonitor.reportHeartbeat();

            return heartbeatMonitor.getHeartbeatTarget();
        } else {
            return null;
        }
    }
}
