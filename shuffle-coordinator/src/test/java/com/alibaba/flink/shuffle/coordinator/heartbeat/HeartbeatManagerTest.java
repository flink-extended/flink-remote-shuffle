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

import com.alibaba.flink.shuffle.coordinator.utils.TestingUtils;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.utils.OneShotLatch;
import com.alibaba.flink.shuffle.core.utils.TestLogger;
import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutor;
import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutorServiceAdapter;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link HeartbeatManager}. */
public class HeartbeatManagerTest extends TestLogger {
    private static final Logger LOG = LoggerFactory.getLogger(HeartbeatManagerTest.class);
    public static final long HEARTBEAT_INTERVAL = 50L;
    public static final long HEARTBEAT_TIMEOUT = 200L;

    /**
     * Tests that regular heartbeat signal triggers the right callback functions in the {@link
     * HeartbeatListener}.
     */
    @Test
    public void testRegularHeartbeat() throws InterruptedException {
        final long heartbeatTimeout = 1000L;
        InstanceID ownInstanceID = new InstanceID("foobar");
        InstanceID targetInstanceID = new InstanceID("barfoo");
        final int outputPayload = 42;
        final ArrayBlockingQueue<String> reportedPayloads = new ArrayBlockingQueue<>(2);
        final TestingHeartbeatListener<String, Integer> heartbeatListener =
                new TestingHeartbeatListenerBuilder<String, Integer>()
                        .setReportPayloadConsumer(
                                (ignored, payload) -> reportedPayloads.offer(payload))
                        .setRetrievePayloadFunction((ignored) -> outputPayload)
                        .createNewTestingHeartbeatListener();

        HeartbeatManagerImpl<String, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        ownInstanceID,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        final ArrayBlockingQueue<Integer> reportedPayloadsHeartbeatTarget =
                new ArrayBlockingQueue<>(2);
        final TestingHeartbeatTarget<Integer> heartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>()
                        .setReceiveHeartbeatConsumer(
                                (ignoredA, payload) ->
                                        reportedPayloadsHeartbeatTarget.offer(payload))
                        .createTestingHeartbeatTarget();

        heartbeatManager.monitorTarget(targetInstanceID, heartbeatTarget);

        final String inputPayload1 = "foobar";
        heartbeatManager.requestHeartbeat(targetInstanceID, inputPayload1);

        assertThat(reportedPayloads.take(), is(inputPayload1));
        assertThat(reportedPayloadsHeartbeatTarget.take(), is(outputPayload));

        final String inputPayload2 = "barfoo";
        heartbeatManager.receiveHeartbeat(targetInstanceID, inputPayload2);
        assertThat(reportedPayloads.take(), is(inputPayload2));
    }

    /** Tests that the heartbeat monitors are updated when receiving a new heartbeat signal. */
    @Test
    public void testHeartbeatMonitorUpdate() {
        long heartbeatTimeout = 1000L;
        InstanceID ownInstanceID = new InstanceID();
        InstanceID targetInstanceID = new InstanceID("barfoo");
        @SuppressWarnings("unchecked")
        HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);
        ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

        doReturn(scheduledFuture)
                .when(scheduledExecutor)
                .schedule(
                        ArgumentMatchers.any(Runnable.class),
                        ArgumentMatchers.anyLong(),
                        ArgumentMatchers.any(TimeUnit.class));

        Object expectedObject = new Object();

        when(heartbeatListener.retrievePayload(ArgumentMatchers.any(InstanceID.class)))
                .thenReturn(CompletableFuture.completedFuture(expectedObject));

        HeartbeatManagerImpl<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout, ownInstanceID, heartbeatListener, scheduledExecutor, LOG);

        @SuppressWarnings("unchecked")
        HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);

        heartbeatManager.monitorTarget(targetInstanceID, heartbeatTarget);

        heartbeatManager.receiveHeartbeat(targetInstanceID, expectedObject);

        verify(scheduledFuture, times(1)).cancel(true);
        verify(scheduledExecutor, times(2))
                .schedule(
                        ArgumentMatchers.any(Runnable.class),
                        ArgumentMatchers.eq(heartbeatTimeout),
                        ArgumentMatchers.eq(TimeUnit.MILLISECONDS));
    }

    /** Tests that a heartbeat timeout is signaled if the heartbeat is not reported in time. */
    @Test
    public void testHeartbeatTimeout() throws Exception {
        int numHeartbeats = 6;
        final int payload = 42;

        InstanceID ownInstanceID = new InstanceID("foobar");
        InstanceID targetInstanceID = new InstanceID("barfoo");

        final CompletableFuture<InstanceID> timeoutFuture = new CompletableFuture<>();
        final TestingHeartbeatListener<Integer, Integer> heartbeatListener =
                new TestingHeartbeatListenerBuilder<Integer, Integer>()
                        .setRetrievePayloadFunction(ignored -> payload)
                        .setNotifyHeartbeatTimeoutConsumer(timeoutFuture::complete)
                        .createNewTestingHeartbeatListener();

        HeartbeatManagerImpl<Integer, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        HEARTBEAT_TIMEOUT,
                        ownInstanceID,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        final HeartbeatTarget<Integer> heartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>().createTestingHeartbeatTarget();

        heartbeatManager.monitorTarget(targetInstanceID, heartbeatTarget);

        for (int i = 0; i < numHeartbeats; i++) {
            heartbeatManager.receiveHeartbeat(targetInstanceID, payload);
            Thread.sleep(HEARTBEAT_INTERVAL);
        }

        assertFalse(timeoutFuture.isDone());

        InstanceID timeoutInstanceID =
                timeoutFuture.get(2 * HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);

        assertEquals(targetInstanceID, timeoutInstanceID);
    }

    /**
     * Tests the heartbeat interplay between the {@link HeartbeatManagerImpl} and the {@link
     * HeartbeatManagerSenderImpl}. The sender should regularly trigger heartbeat requests which are
     * fulfilled by the receiver. Upon stopping the receiver, the sender should notify the heartbeat
     * listener about the heartbeat timeout.
     *
     * @throws Exception when error happen.
     */
    @Test
    public void testHeartbeatCluster() throws Exception {
        InstanceID instanceIDTarget = new InstanceID("foobar");
        InstanceID instanceIDSender = new InstanceID("barfoo");
        final int targetPayload = 42;
        final AtomicInteger numReportPayloadCallsTarget = new AtomicInteger(0);
        final TestingHeartbeatListener<String, Integer> heartbeatListenerTarget =
                new TestingHeartbeatListenerBuilder<String, Integer>()
                        .setRetrievePayloadFunction(ignored -> targetPayload)
                        .setReportPayloadConsumer(
                                (ignoredA, ignoredB) ->
                                        numReportPayloadCallsTarget.incrementAndGet())
                        .createNewTestingHeartbeatListener();

        final String senderPayload = "1337";
        final CompletableFuture<InstanceID> targetHeartbeatTimeoutFuture =
                new CompletableFuture<>();
        final AtomicInteger numReportPayloadCallsSender = new AtomicInteger(0);
        final TestingHeartbeatListener<Integer, String> heartbeatListenerSender =
                new TestingHeartbeatListenerBuilder<Integer, String>()
                        .setRetrievePayloadFunction(ignored -> senderPayload)
                        .setNotifyHeartbeatTimeoutConsumer(targetHeartbeatTimeoutFuture::complete)
                        .setReportPayloadConsumer(
                                (ignoredA, ignoredB) ->
                                        numReportPayloadCallsSender.incrementAndGet())
                        .createNewTestingHeartbeatListener();

        HeartbeatManagerImpl<String, Integer> heartbeatManagerTarget =
                new HeartbeatManagerImpl<>(
                        HEARTBEAT_TIMEOUT,
                        instanceIDTarget,
                        heartbeatListenerTarget,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        HeartbeatManagerSenderImpl<Integer, String> heartbeatManagerSender =
                new HeartbeatManagerSenderImpl<>(
                        HEARTBEAT_INTERVAL,
                        HEARTBEAT_TIMEOUT,
                        instanceIDSender,
                        heartbeatListenerSender,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        heartbeatManagerTarget.monitorTarget(instanceIDSender, heartbeatManagerSender);
        heartbeatManagerSender.monitorTarget(instanceIDTarget, heartbeatManagerTarget);

        Thread.sleep(2 * HEARTBEAT_TIMEOUT);

        assertFalse(targetHeartbeatTimeoutFuture.isDone());

        heartbeatManagerTarget.stop();

        InstanceID timeoutInstanceID =
                targetHeartbeatTimeoutFuture.get(2 * HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);

        assertThat(timeoutInstanceID, is(instanceIDTarget));

        int numberHeartbeats = (int) (2 * HEARTBEAT_TIMEOUT / HEARTBEAT_INTERVAL);

        final Matcher<Integer> numberHeartbeatsMatcher = greaterThanOrEqualTo(numberHeartbeats / 2);
        assertThat(numReportPayloadCallsTarget.get(), is(numberHeartbeatsMatcher));
        assertThat(numReportPayloadCallsSender.get(), is(numberHeartbeatsMatcher));
    }

    /** Tests that after unmonitoring a target, there won't be a timeout triggered. */
    @Test
    public void testTargetUnmonitoring() throws Exception {
        // this might be too aggressive for Travis, let's see...
        long heartbeatTimeout = 50L;
        InstanceID instanceID = new InstanceID("foobar");
        InstanceID targetID = new InstanceID("target");
        final int payload = 42;

        final CompletableFuture<InstanceID> timeoutFuture = new CompletableFuture<>();
        final TestingHeartbeatListener<Integer, Integer> heartbeatListener =
                new TestingHeartbeatListenerBuilder<Integer, Integer>()
                        .setRetrievePayloadFunction(ignored -> payload)
                        .setNotifyHeartbeatTimeoutConsumer(timeoutFuture::complete)
                        .createNewTestingHeartbeatListener();

        HeartbeatManager<Integer, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        instanceID,
                        heartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        final HeartbeatTarget<Integer> heartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>().createTestingHeartbeatTarget();
        heartbeatManager.monitorTarget(targetID, heartbeatTarget);

        heartbeatManager.unmonitorTarget(targetID);

        try {
            timeoutFuture.get(2 * heartbeatTimeout, TimeUnit.MILLISECONDS);
            fail("Timeout should time out.");
        } catch (TimeoutException ignored) {
            // the timeout should not be completed since we unmonitored the target
        }
    }

    /** Tests that the last heartbeat from an unregistered target equals -1. */
    @Test
    public void testLastHeartbeatFromUnregisteredTarget() {
        final long heartbeatTimeout = 100L;
        final InstanceID instanceID = new InstanceID();
        @SuppressWarnings("unchecked")
        final HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);

        HeartbeatManager<?, ?> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        instanceID,
                        heartbeatListener,
                        mock(ScheduledExecutor.class),
                        LOG);

        try {
            assertEquals(-1L, heartbeatManager.getLastHeartbeatFrom(new InstanceID()));
        } finally {
            heartbeatManager.stop();
        }
    }

    /** Tests that we can correctly retrieve the last heartbeat for registered targets. */
    @Test
    public void testLastHeartbeatFrom() {
        final long heartbeatTimeout = 100L;
        final InstanceID instanceID = new InstanceID();
        @SuppressWarnings("unchecked")
        final HeartbeatListener<Object, Object> heartbeatListener = mock(HeartbeatListener.class);
        @SuppressWarnings("unchecked")
        final HeartbeatTarget<Object> heartbeatTarget = mock(HeartbeatTarget.class);
        final InstanceID target = new InstanceID();

        HeartbeatManager<Object, Object> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        instanceID,
                        heartbeatListener,
                        mock(ScheduledExecutor.class),
                        LOG);

        try {
            heartbeatManager.monitorTarget(target, heartbeatTarget);

            assertEquals(0L, heartbeatManager.getLastHeartbeatFrom(target));

            final long currentTime = System.currentTimeMillis();

            heartbeatManager.receiveHeartbeat(target, null);

            assertTrue(heartbeatManager.getLastHeartbeatFrom(target) >= currentTime);
        } finally {
            heartbeatManager.stop();
        }
    }

    /**
     * Tests that the heartbeat target {@link InstanceID} is properly passed to the {@link
     * HeartbeatListener} by the {@link HeartbeatManagerImpl}.
     */
    @Test
    public void testHeartbeatManagerTargetPayload() throws Exception {
        final long heartbeatTimeout = 100L;

        final InstanceID someTargetId = new InstanceID();
        final InstanceID specialTargetId = new InstanceID();

        final Map<InstanceID, Integer> payloads = new HashMap<>(2);
        payloads.put(someTargetId, 0);
        payloads.put(specialTargetId, 1);

        final CompletableFuture<Integer> someHeartbeatPayloadFuture = new CompletableFuture<>();
        final TestingHeartbeatTarget<Integer> someHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>()
                        .setReceiveHeartbeatConsumer(
                                (ignored, payload) -> someHeartbeatPayloadFuture.complete(payload))
                        .createTestingHeartbeatTarget();

        final CompletableFuture<Integer> specialHeartbeatPayloadFuture = new CompletableFuture<>();
        final TestingHeartbeatTarget<Integer> specialHeartbeatTarget =
                new TestingHeartbeatTargetBuilder<Integer>()
                        .setReceiveHeartbeatConsumer(
                                (ignored, payload) ->
                                        specialHeartbeatPayloadFuture.complete(payload))
                        .createTestingHeartbeatTarget();

        final TestingHeartbeatListener<Void, Integer> testingHeartbeatListener =
                new TestingHeartbeatListenerBuilder<Void, Integer>()
                        .setRetrievePayloadFunction(payloads::get)
                        .createNewTestingHeartbeatListener();

        HeartbeatManager<?, Integer> heartbeatManager =
                new HeartbeatManagerImpl<>(
                        heartbeatTimeout,
                        new InstanceID(),
                        testingHeartbeatListener,
                        TestingUtils.defaultScheduledExecutor(),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);
            heartbeatManager.monitorTarget(specialTargetId, specialHeartbeatTarget);

            heartbeatManager.requestHeartbeat(someTargetId, null);
            assertThat(someHeartbeatPayloadFuture.get(), is(payloads.get(someTargetId)));

            heartbeatManager.requestHeartbeat(specialTargetId, null);
            assertThat(specialHeartbeatPayloadFuture.get(), is(payloads.get(specialTargetId)));
        } finally {
            heartbeatManager.stop();
        }
    }

    /**
     * Tests that the heartbeat target {@link InstanceID} is properly passed to the {@link
     * HeartbeatListener} by the {@link HeartbeatManagerSenderImpl}.
     */
    @Test
    public void testHeartbeatManagerSenderTargetPayload() throws Exception {
        final long heartbeatTimeout = 100L;
        final long heartbeatPeriod = 2000L;

        final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor =
                new ScheduledThreadPoolExecutor(1);

        final InstanceID someTargetId = new InstanceID();
        final InstanceID specialTargetId = new InstanceID();

        final OneShotLatch someTargetReceivedLatch = new OneShotLatch();
        final OneShotLatch specialTargetReceivedLatch = new OneShotLatch();

        final TargetDependentHeartbeatReceiver someHeartbeatTarget =
                new TargetDependentHeartbeatReceiver(someTargetReceivedLatch);
        final TargetDependentHeartbeatReceiver specialHeartbeatTarget =
                new TargetDependentHeartbeatReceiver(specialTargetReceivedLatch);

        final int defaultResponse = 0;
        final int specialResponse = 1;

        HeartbeatManager<?, Integer> heartbeatManager =
                new HeartbeatManagerSenderImpl<>(
                        heartbeatPeriod,
                        heartbeatTimeout,
                        new InstanceID(),
                        new TargetDependentHeartbeatSender(
                                specialTargetId, specialResponse, defaultResponse),
                        new ScheduledExecutorServiceAdapter(scheduledThreadPoolExecutor),
                        LOG);

        try {
            heartbeatManager.monitorTarget(someTargetId, someHeartbeatTarget);
            heartbeatManager.monitorTarget(specialTargetId, specialHeartbeatTarget);

            someTargetReceivedLatch.await(5, TimeUnit.SECONDS);
            specialTargetReceivedLatch.await(5, TimeUnit.SECONDS);

            assertEquals(defaultResponse, someHeartbeatTarget.getLastRequestedHeartbeatPayload());
            assertEquals(
                    specialResponse, specialHeartbeatTarget.getLastRequestedHeartbeatPayload());
        } finally {
            heartbeatManager.stop();
            scheduledThreadPoolExecutor.shutdown();
        }
    }

    /** Test {@link HeartbeatTarget} that exposes the last received payload. */
    private static class TargetDependentHeartbeatReceiver implements HeartbeatTarget<Integer> {

        private volatile int lastReceivedHeartbeatPayload = -1;
        private volatile int lastRequestedHeartbeatPayload = -1;

        private final OneShotLatch latch;

        public TargetDependentHeartbeatReceiver() {
            this(new OneShotLatch());
        }

        public TargetDependentHeartbeatReceiver(OneShotLatch latch) {
            this.latch = latch;
        }

        @Override
        public void receiveHeartbeat(InstanceID heartbeatOrigin, Integer heartbeatPayload) {
            this.lastReceivedHeartbeatPayload = heartbeatPayload;
            latch.trigger();
        }

        @Override
        public void requestHeartbeat(InstanceID requestOrigin, Integer heartbeatPayload) {
            this.lastRequestedHeartbeatPayload = heartbeatPayload;
            latch.trigger();
        }

        public int getLastReceivedHeartbeatPayload() {
            return lastReceivedHeartbeatPayload;
        }

        public int getLastRequestedHeartbeatPayload() {
            return lastRequestedHeartbeatPayload;
        }
    }

    /**
     * Test {@link HeartbeatListener} that returns different payloads based on the target {@link
     * InstanceID}.
     */
    private static class TargetDependentHeartbeatSender
            implements HeartbeatListener<Object, Integer> {
        private final InstanceID specialId;
        private final int specialResponse;
        private final int defaultResponse;

        TargetDependentHeartbeatSender(
                InstanceID specialId, int specialResponse, int defaultResponse) {
            this.specialId = specialId;
            this.specialResponse = specialResponse;
            this.defaultResponse = defaultResponse;
        }

        @Override
        public void notifyHeartbeatTimeout(InstanceID instanceID) {}

        @Override
        public void reportPayload(InstanceID instanceID, Object payload) {}

        @Override
        public Integer retrievePayload(InstanceID instanceID) {
            if (instanceID.equals(specialId)) {
                return specialResponse;
            } else {
                return defaultResponse;
            }
        }
    }
}
