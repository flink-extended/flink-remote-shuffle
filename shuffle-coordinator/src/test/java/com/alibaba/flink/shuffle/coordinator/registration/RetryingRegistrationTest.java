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

package com.alibaba.flink.shuffle.coordinator.registration;

import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.core.utils.TestLogger;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.test.TestingRpcService;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the generic retrying registration class, validating the failure, retry, and back-off
 * behavior.
 */
public class RetryingRegistrationTest extends TestLogger {

    private TestingRpcService rpcService;

    @Before
    public void setup() {
        rpcService = new TestingRpcService();
    }

    @After
    public void tearDown() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.stopService().get();
        }
    }

    @Test
    public void testSimpleSuccessfulRegistration() throws Exception {
        final String testId = "laissez les bon temps roulez";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        // an endpoint that immediately returns success
        TestRegistrationGateway testGateway =
                new TestRegistrationGateway(new TestRegistrationSuccess(testId));

        try {
            rpcService.registerGateway(testEndpointAddress, testGateway);

            TestRetryingRegistration registration =
                    new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);
            registration.startRegistration();

            CompletableFuture<Pair<TestRegistrationGateway, TestRegistrationSuccess>> future =
                    registration.getFuture();
            assertNotNull(future);

            // multiple accesses return the same future
            assertEquals(future, registration.getFuture());

            Pair<TestRegistrationGateway, TestRegistrationSuccess> success =
                    future.get(10L, TimeUnit.SECONDS);

            // validate correct invocation and result
            assertEquals(testId, success.getRight().getCorrelationId());
            assertEquals(leaderId, testGateway.getInvocations().take().leaderId());
        } finally {
            testGateway.stop();
        }
    }

    @Test
    public void testPropagateFailures() throws Exception {
        final String testExceptionMessage = "testExceptionMessage";

        // RPC service that fails with exception upon the connection
        RemoteShuffleRpcService rpcService = mock(RemoteShuffleRpcService.class);
        when(rpcService.connectTo(anyString(), any(Class.class)))
                .thenThrow(new RuntimeException(testExceptionMessage));

        TestRetryingRegistration registration =
                new TestRetryingRegistration(rpcService, "testaddress", UUID.randomUUID());
        registration.startRegistration();

        CompletableFuture<?> future = registration.getFuture();
        assertTrue(future.isDone());

        try {
            future.get();

            fail("We expected an ExecutionException.");
        } catch (ExecutionException e) {
            assertEquals(testExceptionMessage, e.getCause().getMessage());
        }
    }

    @Test
    public void testRetryConnectOnFailure() throws Exception {
        final String testId = "laissez les bon temps roulez";
        final UUID leaderId = UUID.randomUUID();

        ExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        TestRegistrationGateway testGateway =
                new TestRegistrationGateway(new TestRegistrationSuccess(testId));

        try {
            // RPC service that fails upon the first connection, but succeeds on the second
            RemoteShuffleRpcService rpcService = mock(RemoteShuffleRpcService.class);
            when(rpcService.connectTo(anyString(), any(Class.class)))
                    .thenReturn(
                            FutureUtils.completedExceptionally(
                                    new Exception(
                                            "test connect failure")), // first connection attempt
                            // fails
                            CompletableFuture.completedFuture(
                                    testGateway) // second connection attempt succeeds
                            );
            when(rpcService.getExecutor()).thenReturn(executor);
            when(rpcService.scheduleRunnable(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                    .thenAnswer(
                            (InvocationOnMock invocation) -> {
                                final Runnable runnable = invocation.getArgument(0);
                                final long delay = invocation.getArgument(1);
                                final TimeUnit timeUnit = invocation.getArgument(2);
                                return Executors.newSingleThreadScheduledExecutor()
                                        .schedule(runnable, delay, timeUnit);
                            });

            TestRetryingRegistration registration =
                    new TestRetryingRegistration(rpcService, "foobar address", leaderId);

            long start = System.currentTimeMillis();

            registration.startRegistration();

            Pair<TestRegistrationGateway, TestRegistrationSuccess> success =
                    registration.getFuture().get(10L, TimeUnit.SECONDS);

            // measure the duration of the registration --> should be longer than the error delay
            long duration = System.currentTimeMillis() - start;

            assertTrue(
                    "The registration should have failed the first time. Thus the duration should be longer than at least a single error delay.",
                    duration > TestRetryingRegistration.DELAY_ON_ERROR);

            // validate correct invocation and result
            assertEquals(testId, success.getRight().getCorrelationId());
            assertEquals(leaderId, testGateway.getInvocations().take().leaderId());
        } finally {
            testGateway.stop();
        }
    }

    @Test(timeout = 10000)
    public void testRetriesOnTimeouts() throws Exception {
        final String testId = "rien ne va plus";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        // an endpoint that immediately returns futures with timeouts before returning a successful
        // future
        TestRegistrationGateway testGateway =
                new TestRegistrationGateway(
                        null, // timeout
                        null, // timeout
                        new TestRegistrationSuccess(testId) // success
                        );

        try {
            rpcService.registerGateway(testEndpointAddress, testGateway);
            TestRetryingRegistration registration =
                    new TestRetryingRegistration(
                            rpcService,
                            testEndpointAddress,
                            leaderId,
                            new RetryingRegistrationConfiguration(
                                    15000L, // make sure that we timeout in case of an error
                                    15000L));
            registration.startRegistration();

            CompletableFuture<Pair<TestRegistrationGateway, TestRegistrationSuccess>> future =
                    registration.getFuture();
            Pair<TestRegistrationGateway, TestRegistrationSuccess> success =
                    future.get(10L, TimeUnit.SECONDS);

            // validate correct invocation and result
            assertEquals(testId, success.getRight().getCorrelationId());
            assertEquals(leaderId, testGateway.getInvocations().take().leaderId());
        } finally {
            testGateway.stop();
        }
    }

    @Test
    public void testDecline() throws Exception {
        final String testId = "qui a coupe le fromage";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        TestRegistrationGateway testGateway =
                new TestRegistrationGateway(
                        null, // timeout
                        new RegistrationResponse.Decline("no reason "),
                        null, // timeout
                        new TestRegistrationSuccess(testId) // success
                        );

        try {
            rpcService.registerGateway(testEndpointAddress, testGateway);

            TestRetryingRegistration registration =
                    new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);
            registration.startRegistration();

            CompletableFuture<Pair<TestRegistrationGateway, TestRegistrationSuccess>> future =
                    registration.getFuture();
            Pair<TestRegistrationGateway, TestRegistrationSuccess> success =
                    future.get(10L, TimeUnit.SECONDS);

            // validate correct invocation and result
            assertEquals(testId, success.getRight().getCorrelationId());
            assertEquals(leaderId, testGateway.getInvocations().take().leaderId());
        } finally {
            testGateway.stop();
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testRetryOnError() throws Exception {
        final String testId = "Petit a petit, l'oiseau fait son nid";
        final String testEndpointAddress = "<test-address>";
        final UUID leaderId = UUID.randomUUID();

        // gateway that upon calls first responds with a failure, then with a success
        TestRegistrationGateway testGateway = mock(TestRegistrationGateway.class);

        when(testGateway.registrationCall(any(UUID.class)))
                .thenReturn(
                        FutureUtils.completedExceptionally(new Exception("test exception")),
                        CompletableFuture.completedFuture(new TestRegistrationSuccess(testId)));

        rpcService.registerGateway(testEndpointAddress, testGateway);

        TestRetryingRegistration registration =
                new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);

        long started = System.nanoTime();
        registration.startRegistration();

        CompletableFuture<Pair<TestRegistrationGateway, TestRegistrationSuccess>> future =
                registration.getFuture();
        Pair<TestRegistrationGateway, TestRegistrationSuccess> success =
                future.get(10, TimeUnit.SECONDS);

        long finished = System.nanoTime();
        long elapsedMillis = (finished - started) / 1000000;

        assertEquals(testId, success.getRight().getCorrelationId());

        // validate that some retry-delay / back-off behavior happened
        assertTrue(
                "retries did not properly back off",
                elapsedMillis >= TestRetryingRegistration.DELAY_ON_ERROR);
    }

    @Test
    public void testCancellation() throws Exception {
        final String testEndpointAddress = "my-test-address";
        final UUID leaderId = UUID.randomUUID();

        CompletableFuture<RegistrationResponse> result = new CompletableFuture<>();

        TestRegistrationGateway testGateway = mock(TestRegistrationGateway.class);
        when(testGateway.registrationCall(any(UUID.class))).thenReturn(result);

        rpcService.registerGateway(testEndpointAddress, testGateway);

        TestRetryingRegistration registration =
                new TestRetryingRegistration(rpcService, testEndpointAddress, leaderId);
        registration.startRegistration();

        // cancel and fail the current registration attempt
        registration.cancel();
        result.completeExceptionally(new TimeoutException());

        // there should not be a second registration attempt
        verify(testGateway, atMost(1)).registrationCall(any(UUID.class));
    }

    // ------------------------------------------------------------------------
    //  test registration
    // ------------------------------------------------------------------------

    static class TestRegistrationSuccess extends RegistrationResponse.Success {
        private static final long serialVersionUID = 5542698790917150604L;

        private final String correlationId;

        public TestRegistrationSuccess(String correlationId) {
            this.correlationId = correlationId;
        }

        public String getCorrelationId() {
            return correlationId;
        }
    }

    static class TestRetryingRegistration
            extends RetryingRegistration<UUID, TestRegistrationGateway, TestRegistrationSuccess> {

        // we use shorter timeouts here to speed up the tests
        static final long DELAY_ON_ERROR = 200;
        static final long DELAY_ON_DECLINE = 200;
        static final RetryingRegistrationConfiguration RETRYING_REGISTRATION_CONFIGURATION =
                new RetryingRegistrationConfiguration(DELAY_ON_ERROR, DELAY_ON_DECLINE);

        public TestRetryingRegistration(
                RemoteShuffleRpcService rpcService, String targetAddress, UUID leaderId) {
            this(rpcService, targetAddress, leaderId, RETRYING_REGISTRATION_CONFIGURATION);
        }

        public TestRetryingRegistration(
                RemoteShuffleRpcService rpcService,
                String targetAddress,
                UUID leaderId,
                RetryingRegistrationConfiguration retryingRegistrationConfiguration) {
            super(
                    LoggerFactory.getLogger(RetryingRegistrationTest.class),
                    rpcService,
                    "TestEndpoint",
                    TestRegistrationGateway.class,
                    targetAddress,
                    leaderId,
                    retryingRegistrationConfiguration);
        }

        @Override
        protected CompletableFuture<RegistrationResponse> invokeRegistration(
                TestRegistrationGateway gateway, UUID leaderId) {
            return gateway.registrationCall(leaderId);
        }
    }
}
