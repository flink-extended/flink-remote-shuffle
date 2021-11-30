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

package com.alibaba.flink.shuffle.coordinator.worker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.common.utils.FatalErrorExitUtils;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.common.utils.JvmShutdownSafeguard;
import com.alibaba.flink.shuffle.common.utils.SignalHandler;
import com.alibaba.flink.shuffle.common.utils.StringUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServicesUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServiceUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils;
import com.alibaba.flink.shuffle.coordinator.utils.EnvironmentInformation;
import com.alibaba.flink.shuffle.coordinator.utils.LeaderRetrievalUtils;
import com.alibaba.flink.shuffle.coordinator.worker.metastore.LocalShuffleMetaStore;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;
import com.alibaba.flink.shuffle.storage.datastore.PartitionedDataStoreImpl;
import com.alibaba.flink.shuffle.storage.utils.StorageConfigParseUtils;
import com.alibaba.flink.shuffle.transfer.NettyConfig;
import com.alibaba.flink.shuffle.transfer.NettyServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils.STARTUP_FAILURE_RETURN_CODE;

/** The entrypoint of the ShuffleWorker. */
public class ShuffleWorkerRunner implements FatalErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleWorkerRunner.class);

    private static final Duration LOOKUP_TIMEOUT_DURATION = Duration.ofSeconds(30);

    private static final int SUCCESS_EXIT_CODE = 0;

    private static final int FAILURE_EXIT_CODE = 1;

    private final Object lock = new Object();

    private final RemoteShuffleRpcService rpcService;

    private final HaServices haServices;

    private final ShuffleWorker shuffleWorker;

    private final CompletableFuture<Result> terminationFuture = new CompletableFuture<>();

    private boolean shutdown;

    public ShuffleWorkerRunner(Configuration configuration) throws Exception {
        checkArgument(configuration != null, "Must be not null.");

        haServices = HaServiceUtils.createHAServices(configuration);

        HeartbeatServices heartbeatServices =
                HeartbeatServicesUtils.createManagerWorkerHeartbeatServices(configuration);

        MetricUtils.startWorkerMetricSystem(configuration);

        AkkaRpcServiceUtils.loadRpcSystem(configuration);
        this.rpcService =
                AkkaRpcServiceUtils.createRemoteRpcService(
                        configuration,
                        determineShuffleWorkerBindAddress(configuration, haServices),
                        configuration.getString(WorkerOptions.RPC_PORT),
                        configuration.getString(WorkerOptions.BIND_HOST),
                        Optional.ofNullable(configuration.getInteger(WorkerOptions.RPC_BIND_PORT)));

        this.shuffleWorker =
                createShuffleWorker(configuration, rpcService, haServices, heartbeatServices, this);
        shuffleWorker
                .getTerminationFuture()
                .whenComplete(
                        ((unused, throwable) -> {
                            synchronized (lock) {
                                if (!shutdown) {
                                    onFatalError(
                                            new Exception(
                                                    "Unexpected termination of the ShuffleWorker.",
                                                    throwable));
                                }
                            }
                        }));
    }

    private static String determineShuffleWorkerBindAddress(
            Configuration configuration, HaServices haServices) throws Exception {

        final String configuredShuffleWorkerHostname = configuration.getString(WorkerOptions.HOST);

        if (configuredShuffleWorkerHostname != null) {
            LOG.info(
                    "Using configured hostname/address for ShuffleWorker: {}.",
                    configuredShuffleWorkerHostname);
            return configuredShuffleWorkerHostname;
        } else {
            return determineShuffleWorkerBindAddressByConnectingToShuffleManager(
                    configuration, haServices);
        }
    }

    private static String determineShuffleWorkerBindAddressByConnectingToShuffleManager(
            Configuration configuration, HaServices haServices) throws Exception {
        final InetAddress shuffleWorkerAddress =
                LeaderRetrievalUtils.findConnectingAddress(
                        haServices.createLeaderRetrievalService(
                                HaServices.LeaderReceptor.SHUFFLE_WORKER),
                        LOOKUP_TIMEOUT_DURATION);

        LOG.info(
                "ShuffleWorker will use hostname/address '{}' ({}) for communication.",
                shuffleWorkerAddress.getHostName(),
                shuffleWorkerAddress.getHostAddress());

        HostBindPolicy bindPolicy =
                HostBindPolicy.fromString(configuration.getString(WorkerOptions.HOST_BIND_POLICY));
        return bindPolicy == HostBindPolicy.IP
                ? shuffleWorkerAddress.getHostAddress()
                : shuffleWorkerAddress.getHostName();
    }

    static InstanceID getShuffleWorkerID(String rpcAddress, int rpcPort) throws Exception {
        String randomString = CommonUtils.randomHexString(16);
        return new InstanceID(
                StringUtils.isNullOrWhitespaceOnly(rpcAddress)
                        ? InetAddress.getLocalHost().getHostName() + "-" + randomString
                        : rpcAddress + ":" + rpcPort + "-" + randomString);
    }

    // export the termination future for caller to know it is terminated
    public CompletableFuture<Result> getTerminationFuture() {
        return terminationFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  Lifecycle management
    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "Shuffle Worker", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if (maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        try {
            Configuration configuration = ClusterEntrypointUtils.parseParametersOrExit(args);
            runShuffleWorker(configuration);
        } catch (Throwable t) {
            LOG.error("ShuffleWorker initialization failed.", t);
            FatalErrorExitUtils.exitProcessIfNeeded(STARTUP_FAILURE_RETURN_CODE);
        }
    }

    public static ShuffleWorkerRunner runShuffleWorker(Configuration configuration)
            throws Exception {
        ShuffleWorkerRunner shuffleWorkerRunner = new ShuffleWorkerRunner(configuration);
        shuffleWorkerRunner.start();
        LOG.info("Shuffle worker runner is started");
        return shuffleWorkerRunner;
    }

    public static ShuffleWorker createShuffleWorker(
            Configuration configuration,
            RemoteShuffleRpcService rpcService,
            HaServices haServices,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        InstanceID workerID = getShuffleWorkerID(rpcService.getAddress(), rpcService.getPort());

        ShuffleWorkerConfiguration shuffleWorkerConfiguration =
                ShuffleWorkerConfiguration.fromConfiguration(configuration);

        String directories = configuration.getString(StorageOptions.STORAGE_LOCAL_DATA_DIRS);
        if (directories == null || directories.trim().isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "The data dir '%s' configured by '%s' is not valid.",
                            directories, StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
        }

        List<String> allPaths =
                StorageConfigParseUtils.parseStoragePaths(directories).getAllPaths();
        LocalShuffleMetaStore fileSystemShuffleMetaStore =
                new LocalShuffleMetaStore(new HashSet<>(allPaths));

        PartitionedDataStoreImpl dataStore =
                new PartitionedDataStoreImpl(configuration, fileSystemShuffleMetaStore);

        // Initialize the data partitions on startup
        int recoveredCount = 0;
        int failedCount = 0;
        for (DataPartitionMeta dataPartitionMeta :
                fileSystemShuffleMetaStore.getAllDataPartitionMetas()) {
            try {
                dataStore.addDataPartition(dataPartitionMeta);
                recoveredCount++;
            } catch (Throwable t) {
                LOG.warn(
                        "Failed to initialize the data partition {}-{}-{}",
                        dataPartitionMeta.getJobID(),
                        dataPartitionMeta.getDataSetID(),
                        dataPartitionMeta.getDataPartitionID());
                failedCount++;
            }
        }
        LOG.info(
                "Recovered {} partitions successfully and {} partitions with failure",
                recoveredCount,
                failedCount);

        NettyConfig nettyConfig = new NettyConfig(configuration);
        NettyServer nettyServer = new NettyServer(dataStore, nettyConfig);
        nettyServer.start();

        ShuffleWorkerLocation shuffleWorkerLocation =
                new ShuffleWorkerLocation(
                        rpcService.getAddress(), nettyConfig.getServerPort(), workerID);

        return new ShuffleWorker(
                rpcService,
                shuffleWorkerConfiguration,
                haServices,
                heartbeatServices,
                fatalErrorHandler,
                shuffleWorkerLocation,
                fileSystemShuffleMetaStore,
                dataStore,
                nettyServer);
    }

    public void start() {
        shuffleWorker.start();
    }

    // --------------------------------------------------------------------------------------------
    //  Static entry point
    // --------------------------------------------------------------------------------------------

    @Override
    public void onFatalError(Throwable exception) {
        LOG.error(
                "Fatal error occurred while executing the ShuffleWorker. Shutting it down...",
                exception);

        // In case of the Metaspace OutOfMemoryError, we expect that the graceful shutdown is
        // possible,
        // as it does not usually require more class loading to fail again with the Metaspace
        // OutOfMemoryError.
        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)
                && !ExceptionUtils.isMetaspaceOutOfMemoryError(exception)) {
            terminateJVM();
        } else {
            closeAsync(Result.FAILURE);
        }
    }

    private void terminateJVM() {
        FatalErrorExitUtils.exitProcessIfNeeded(FAILURE_EXIT_CODE);
    }

    public void close() throws Exception {
        try {
            closeAsync(Result.SUCCESS).get();
        } catch (ExecutionException e) {
            ExceptionUtils.rethrowException(
                    ExceptionUtils.stripException(e, ExecutionException.class));
        }
    }

    private CompletableFuture<Result> closeAsync(Result terminationResult) {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;

                final CompletableFuture<Void> shuffleWorkerTerminationFuture =
                        shuffleWorker.closeAsync();

                final CompletableFuture<Void> serviceTerminationFuture =
                        FutureUtils.composeAfterwards(
                                shuffleWorkerTerminationFuture, this::shutDownServices);

                serviceTerminationFuture.whenComplete(
                        (Void ignored, Throwable throwable) -> {
                            if (throwable != null) {
                                terminationFuture.completeExceptionally(throwable);
                            } else {
                                terminationFuture.complete(terminationResult);
                            }
                        });
            }
        }

        return terminationFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  Static utilities
    // --------------------------------------------------------------------------------------------

    private CompletableFuture<Void> shutDownServices() {
        synchronized (lock) {
            Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
            Throwable exception = null;

            try {
                haServices.close();
            } catch (Throwable throwable) {
                exception = throwable;
                LOG.error("Failed to close HA service.", throwable);
            }

            terminationFutures.add(rpcService.stopService());

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            MetricUtils.stopMetricSystem();

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    /** The result of running Shuffle Worker. */
    public enum Result {
        SUCCESS(SUCCESS_EXIT_CODE),

        FAILURE(FAILURE_EXIT_CODE);

        private final int exitCode;

        Result(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
