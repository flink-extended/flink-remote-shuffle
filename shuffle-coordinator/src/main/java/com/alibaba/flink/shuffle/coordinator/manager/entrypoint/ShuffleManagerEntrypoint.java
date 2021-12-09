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

package com.alibaba.flink.shuffle.coordinator.manager.entrypoint;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.functions.AutoCloseableAsync;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServicesUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServiceUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManager;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.AssignmentTrackerImpl;
import com.alibaba.flink.shuffle.coordinator.metrics.MetricsRestHandler;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;
import com.alibaba.flink.shuffle.rest.RestService;
import com.alibaba.flink.shuffle.rest.RestUtil;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** The entrypoint class for the {@link ShuffleManager}. */
public class ShuffleManagerEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

    protected static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerEntrypoint.class);

    protected static final int NORMAL_RETURN_CODE = 0;
    protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
    protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

    private static final long INITIALIZATION_SHUTDOWN_TIMEOUT = 30000L;

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    private final RestService restService;

    private final Configuration configuration;

    private final CompletableFuture<Void> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    private final HaServices haServices;

    private final RemoteShuffleRpcService shuffleRpcService;

    private final ExecutorService ioExecutor;

    private final ShuffleManager shuffleManager;

    public ShuffleManagerEntrypoint(Configuration configuration) throws Exception {
        this.configuration = checkNotNull(configuration);

        this.terminationFuture = new CompletableFuture<>();

        AkkaRpcServiceUtils.loadRpcSystem(configuration);
        this.shuffleRpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration,
                        configuration.getString(ManagerOptions.RPC_ADDRESS),
                        String.valueOf(configuration.getInteger(ManagerOptions.RPC_PORT)),
                        configuration.getString(ManagerOptions.RPC_BIND_ADDRESS),
                        Optional.ofNullable(
                                configuration.getInteger(ManagerOptions.RPC_BIND_PORT)));

        MetricUtils.startMetricSystem(configuration);
        this.restService = RestUtil.startManagerRestService(configuration);
        restService.registerHandler(new MetricsRestHandler());

        // update the configuration used to create the high availability services
        configuration.setString(ManagerOptions.RPC_ADDRESS, shuffleRpcService.getAddress());
        configuration.setInteger(ManagerOptions.RPC_PORT, shuffleRpcService.getPort());

        this.ioExecutor = Executors.newFixedThreadPool(1, new ExecutorThreadFactory("cluster-io"));

        this.haServices = HaServiceUtils.createHAServices(configuration);

        HeartbeatServices workerHeartbeatServices =
                HeartbeatServicesUtils.createManagerWorkerHeartbeatServices(configuration);
        HeartbeatServices jobHeartbeatServices =
                HeartbeatServicesUtils.createManagerJobHeartbeatServices(configuration);

        this.shuffleManager =
                new ShuffleManager(
                        shuffleRpcService,
                        new InstanceID(),
                        haServices,
                        this,
                        ioExecutor,
                        jobHeartbeatServices,
                        workerHeartbeatServices,
                        new AssignmentTrackerImpl());
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    // --------------------------------------------------------------------------------------------
    //  Lifecycle management
    // --------------------------------------------------------------------------------------------

    public static void runShuffleManagerEntrypoint(
            ShuffleManagerEntrypoint shuffleManagerEntrypoint) {
        final String clusterEntrypointName = shuffleManagerEntrypoint.getClass().getSimpleName();
        try {
            shuffleManagerEntrypoint.start();
        } catch (Exception e) {
            LOG.error(
                    String.format("Could not start cluster entrypoint %s.", clusterEntrypointName),
                    e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }

        shuffleManagerEntrypoint
                .getTerminationFuture()
                .whenComplete(
                        (ignored, throwable) -> {
                            int returnCode =
                                    throwable != null
                                            ? RUNTIME_FAILURE_RETURN_CODE
                                            : NORMAL_RETURN_CODE;

                            LOG.info(
                                    "Terminating cluster entrypoint process {} with exit code {}.",
                                    clusterEntrypointName,
                                    returnCode,
                                    throwable);
                            System.exit(returnCode);
                        });
    }

    public void start() throws Exception {
        LOG.info("Starting {}.", getClass().getSimpleName());

        try {
            runCluster();
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

            try {
                // clean up any partial state
                shutDownAsync(ExceptionUtils.stringifyException(strippedThrowable))
                        .get(INITIALIZATION_SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }

            throw new ShuffleException(
                    String.format(
                            "Failed to initialize the cluster entrypoint %s.",
                            getClass().getSimpleName()),
                    strippedThrowable);
        }
    }

    private void runCluster() {
        synchronized (lock) {
            shuffleManager.start();

            shuffleManager
                    .getTerminationFuture()
                    .whenComplete(
                            (applicationStatus, throwable) -> {
                                if (throwable != null) {
                                    shutDownAsync(ExceptionUtils.stringifyException(throwable));
                                } else {
                                    shutDownAsync(null);
                                }
                            });
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return shutDownAsync("Cluster entrypoint has been closed externally.")
                .thenAccept(ignored -> {});
    }

    protected CompletableFuture<Void> stopClusterServices() {
        synchronized (lock) {
            Throwable exception = null;

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (haServices != null) {
                try {
                    haServices.close();
                } catch (Throwable throwable) {
                    exception = throwable;
                    LOG.error("Failed to close HA service.", throwable);
                }
            }

            if (ioExecutor != null) {
                try {
                    ioExecutor.shutdown();
                } catch (Throwable throwable) {
                    exception = exception == null ? throwable : exception;
                    LOG.error("Failed to close executor service.", throwable);
                }
            }

            if (shuffleRpcService != null) {
                terminationFutures.add(shuffleRpcService.stopService());
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            restService.stop();

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    public void onFatalError(Throwable exception) {
        LOG.error("Fatal error occurred in the cluster entrypoint.", exception);
        System.exit(RUNTIME_FAILURE_RETURN_CODE);
    }

    // --------------------------------------------------
    // Internal methods
    // --------------------------------------------------

    public CompletableFuture<Void> getTerminationFuture() {
        return terminationFuture;
    }

    // --------------------------------------------------
    // Helper methods
    // --------------------------------------------------

    private CompletableFuture<Void> shutDownAsync(@Nullable String diagnostics) {
        if (isShutDown.compareAndSet(false, true)) {
            LOG.info("Shutting {} down. Diagnostics {}.", getClass().getSimpleName(), diagnostics);

            stopClusterServices()
                    .whenComplete(
                            (Void ignored2, Throwable serviceThrowable) -> {
                                if (serviceThrowable != null) {
                                    terminationFuture.completeExceptionally(serviceThrowable);
                                } else {
                                    terminationFuture.complete(null);
                                }
                            });
        }

        return terminationFuture;
    }
}
