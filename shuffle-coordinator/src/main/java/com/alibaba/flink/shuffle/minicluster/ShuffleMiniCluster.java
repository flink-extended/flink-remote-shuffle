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

package com.alibaba.flink.shuffle.minicluster;

import com.alibaba.flink.shuffle.client.ShuffleManagerClient;
import com.alibaba.flink.shuffle.client.ShuffleManagerClientConfiguration;
import com.alibaba.flink.shuffle.client.ShuffleManagerClientImpl;
import com.alibaba.flink.shuffle.client.ShuffleWorkerStatusListener;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.functions.AutoCloseableAsync;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.common.utils.NetUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServicesUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServiceUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManager;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.AssignmentTrackerImpl;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorker;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.RestOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** MiniCluster to execute shuffle locally. */
public class ShuffleMiniCluster implements AutoCloseableAsync {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleMiniCluster.class);

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    /** The configuration for this mini cluster. */
    private final ShuffleMiniClusterConfiguration miniClusterConfiguration;

    private static final int RETRY_TIMES_GET_WORKER_NUMBER = 5;

    private ShuffleManager shuffleManager;

    @GuardedBy("lock")
    private final List<ShuffleWorker> shuffleWorkers;

    private final TerminatingFatalErrorHandlerFactory
            shuffleWorkerTerminatingFatalErrorHandlerFactory =
                    new TerminatingFatalErrorHandlerFactory();

    private CompletableFuture<Void> terminationFuture;

    @GuardedBy("lock")
    private final Collection<RemoteShuffleRpcService> rpcServices;

    @GuardedBy("lock")
    private ScheduledExecutorService ioExecutor;

    @GuardedBy("lock")
    private HeartbeatServices workerHeartbeatServices;

    @GuardedBy("lock")
    private HeartbeatServices jobHeartbeatServices;

    @GuardedBy("lock")
    private RpcServiceFactory shuffleWorkerRpcServiceFactory;

    @GuardedBy("lock")
    private RpcServiceFactory shuffleManagerRpcServiceFactory;

    @GuardedBy("lock")
    private RpcServiceFactory shuffleMangerClientRpcServiceFactory;

    /** Flag marking the mini cluster as started/running. */
    private volatile boolean running;

    // ------------------------------------------------------------------------

    /**
     * Creates a new remote shuffle mini cluster based on the given configuration.
     *
     * @param miniClusterConfiguration The configuration for the mini cluster
     */
    public ShuffleMiniCluster(ShuffleMiniClusterConfiguration miniClusterConfiguration) {
        this.miniClusterConfiguration = checkNotNull(miniClusterConfiguration);
        Configuration configuration = miniClusterConfiguration.getConfiguration();
        configuration.setInteger(RestOptions.REST_MANAGER_BIND_PORT, NetUtils.getAvailablePort());

        this.rpcServices =
                new ArrayList<>(
                        1
                                + 1
                                + miniClusterConfiguration
                                        .getNumShuffleWorkers()); // common + manager + workers

        this.terminationFuture = CompletableFuture.completedFuture(null);
        running = false;

        this.shuffleWorkers = new ArrayList<>(miniClusterConfiguration.getNumShuffleWorkers());
    }

    // ------------------------------------------------------------------------
    //  life cycle
    // ------------------------------------------------------------------------

    /** Checks if the mini cluster was started and is running. */
    public boolean isRunning() {
        return running;
    }

    /**
     * Starts the mini cluster, based on the configured properties.
     *
     * @throws Exception This method passes on any exception that occurs during the startup of the
     *     mini cluster.
     */
    public void start() throws Exception {
        synchronized (lock) {
            checkState(!running, "MiniCluster is already running");

            LOG.info("Starting the shuffle mini cluster.");
            LOG.debug("Using configuration {}", miniClusterConfiguration);

            Configuration configuration = miniClusterConfiguration.getConfiguration();

            try {
                // bring up all the RPC services
                LOG.info("Starting RPC Service(s)");

                // start a new service per component, possibly with custom bind addresses
                final String shuffleManagerExternalAddress =
                        miniClusterConfiguration.getShuffleManagerExternalAddress();
                final String shuffleWorkerExternalAddress =
                        miniClusterConfiguration.getShuffleWorkerExternalAddress();
                final String shuffleManagerExternalPortRange =
                        miniClusterConfiguration.getShuffleManagerExternalPortRange();
                final String shuffleWorkerExternalPortRange =
                        miniClusterConfiguration.getShuffleWorkerExternalPortRange();
                final String shuffleManagerBindAddress =
                        miniClusterConfiguration.getShuffleManagerBindAddress();
                final String shuffleWorkerBindAddress =
                        miniClusterConfiguration.getShuffleWorkerBindAddress();

                shuffleWorkerRpcServiceFactory =
                        new DedicatedRpcServiceFactory(
                                configuration,
                                shuffleWorkerExternalAddress,
                                shuffleWorkerExternalPortRange,
                                shuffleWorkerBindAddress);

                shuffleManagerRpcServiceFactory =
                        new DedicatedRpcServiceFactory(
                                configuration,
                                shuffleManagerExternalAddress,
                                shuffleManagerExternalPortRange,
                                shuffleManagerBindAddress);

                shuffleMangerClientRpcServiceFactory =
                        new DedicatedRpcServiceFactory(
                                configuration, null, "0", shuffleManagerBindAddress);

                ioExecutor =
                        Executors.newSingleThreadScheduledExecutor(
                                new ExecutorThreadFactory("mini-cluster-io"));

                workerHeartbeatServices =
                        HeartbeatServicesUtils.createManagerWorkerHeartbeatServices(configuration);
                jobHeartbeatServices =
                        HeartbeatServicesUtils.createManagerJobHeartbeatServices(configuration);

                configuration.setMemorySize(
                        StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, MemorySize.ZERO);

                AkkaRpcServiceUtils.loadRpcSystem(configuration);
                startShuffleManager();

                startShuffleWorkers();

                LOG.info("Waiting for all the workers to register.");
                int retryCount = 0;
                while (retryCount < RETRY_TIMES_GET_WORKER_NUMBER) {
                    boolean getWorkerNumSuccess = false;
                    try {
                        LOG.info(
                                "Retrying to get number of registered workers, retry times: "
                                        + retryCount);
                        while (shuffleManager
                                        .getNumberOfRegisteredWorkers()
                                        .get(30, TimeUnit.SECONDS)
                                < miniClusterConfiguration.getNumShuffleWorkers()) {
                            Thread.sleep(500);
                        }
                        getWorkerNumSuccess = true;
                    } catch (Exception e) {
                        LOG.error("Failed to get number of registered workers, ", e);
                    }
                    if (getWorkerNumSuccess) {
                        break;
                    }
                    retryCount++;
                }
                LOG.info("All the workers have registered.");
            } catch (Exception e) {
                // cleanup everything
                try {
                    close();
                } catch (Exception ee) {
                    e.addSuppressed(ee);
                }
                throw e;
            }

            // create a new termination future
            terminationFuture = new CompletableFuture<>();

            // now officially mark this as running
            running = true;

            LOG.info("Shuffle mini cluster started successfully");
        }
    }

    private HaServices createHaService(Configuration configuration) throws Exception {
        LOG.info("Starting high-availability services");
        return HaServiceUtils.createHAServices(configuration);
    }

    /**
     * Shuts down the mini cluster, failing all currently executing jobs. The mini cluster can be
     * started again by calling the {@link #start()} method again.
     *
     * <p>This method shuts down all started services and components, even if an exception occurs in
     * the process of shutting down some component.
     *
     * @return Future which is completed once the MiniCluster has been completely shut down
     */
    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (running) {
                LOG.info("Shutting down the shuffle mini cluster.");
                try {
                    final int numComponents = 1 + miniClusterConfiguration.getNumShuffleWorkers();
                    final Collection<CompletableFuture<Void>> componentTerminationFutures =
                            new ArrayList<>(numComponents);

                    componentTerminationFutures.addAll(terminateShuffleWorkers());

                    componentTerminationFutures.add(terminateShuffleManager());

                    final FutureUtils.ConjunctFuture<Void> componentsTerminationFuture =
                            FutureUtils.completeAll(componentTerminationFutures);

                    final CompletableFuture<Void> rpcServicesTerminationFuture =
                            FutureUtils.composeAfterwards(
                                    componentsTerminationFuture, this::terminateRpcServices);

                    final CompletableFuture<Void> executorsTerminationFuture =
                            FutureUtils.composeAfterwards(
                                    rpcServicesTerminationFuture, this::terminateExecutors);

                    executorsTerminationFuture.whenComplete(
                            (Void ignored, Throwable throwable) -> {
                                if (throwable != null) {
                                    terminationFuture.completeExceptionally(throwable);
                                } else {
                                    terminationFuture.complete(null);
                                }
                            });
                } finally {
                    running = false;
                }
            }

            return terminationFuture;
        }
    }

    @GuardedBy("lock")
    private void startShuffleWorkers() throws Exception {
        final int numShuffleWorkers = miniClusterConfiguration.getNumShuffleWorkers();

        LOG.info("Starting {} ShuffleWorkers(s)", numShuffleWorkers);

        for (int i = 0; i < numShuffleWorkers; i++) {
            startShuffleWorker();
        }
    }

    private void startShuffleWorker() throws Exception {
        synchronized (lock) {
            Configuration configuration = miniClusterConfiguration.getConfiguration();

            // Choose a random rpc port for the configuration
            Configuration workerConfiguration = new Configuration(configuration);
            workerConfiguration.setInteger(
                    TransferOptions.SERVER_DATA_PORT, NetUtils.getAvailablePort());
            workerConfiguration.setInteger(
                    RestOptions.REST_WORKER_BIND_PORT, NetUtils.getAvailablePort());
            workerConfiguration.setMemorySize(
                    MemoryOptions.MEMORY_SIZE_FOR_DATA_READING,
                    MemoryOptions.MIN_VALID_MEMORY_SIZE);
            workerConfiguration.setMemorySize(
                    MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING,
                    MemoryOptions.MIN_VALID_MEMORY_SIZE);

            RemoteShuffleRpcService rpcService = shuffleWorkerRpcServiceFactory.createRpcService();
            HaServices haServices = createHaService(miniClusterConfiguration.getConfiguration());

            ShuffleWorker shuffleWorker =
                    ShuffleWorkerRunner.createShuffleWorker(
                            workerConfiguration,
                            rpcService,
                            haServices,
                            workerHeartbeatServices,
                            shuffleWorkerTerminatingFatalErrorHandlerFactory.create(
                                    shuffleWorkers.size()));

            shuffleWorker.start();
            shuffleWorkers.add(shuffleWorker);
        }
    }

    @GuardedBy("lock")
    private Collection<? extends CompletableFuture<Void>> terminateShuffleWorkers() {
        final Collection<CompletableFuture<Void>> terminationFutures =
                new ArrayList<>(shuffleWorkers.size());
        for (int i = 0; i < shuffleWorkers.size(); i++) {
            terminationFutures.add(terminateShuffleWorker(i));
        }

        return terminationFutures;
    }

    @Nonnull
    private CompletableFuture<Void> terminateShuffleWorker(int index) {
        synchronized (lock) {
            final ShuffleWorker shuffleWorker = shuffleWorkers.get(index);
            return shuffleWorker.closeAsync();
        }
    }

    private void startShuffleManager() throws Exception {
        RemoteShuffleRpcService rpcService = shuffleManagerRpcServiceFactory.createRpcService();
        HaServices haServices = createHaService(miniClusterConfiguration.getConfiguration());

        shuffleManager =
                new ShuffleManager(
                        rpcService,
                        new InstanceID("Shuffle manager"),
                        haServices,
                        new ShutDownFatalErrorHandler(),
                        ioExecutor,
                        jobHeartbeatServices,
                        workerHeartbeatServices,
                        new AssignmentTrackerImpl(miniClusterConfiguration.getConfiguration()));

        shuffleManager.start();
    }

    public ShuffleManager getShuffleManager() {
        return shuffleManager;
    }

    protected CompletableFuture<Void> terminateShuffleManager() {
        synchronized (lock) {
            return shuffleManager.closeAsync();
        }
    }

    // ------------------------------------------------------------------------
    //  factories - can be overridden by subclasses to alter behavior
    // ------------------------------------------------------------------------

    /**
     * Factory method to instantiate the remote RPC service.
     *
     * @param configuration shuffle configuration.
     * @param externalAddress The external address to access the RPC service.
     * @param externalPortRange The external port range to access the RPC service.
     * @param bindAddress The address to bind the RPC service to.
     * @return The instantiated RPC service
     */
    protected RemoteShuffleRpcService createRemoteRpcService(
            Configuration configuration,
            String externalAddress,
            String externalPortRange,
            String bindAddress)
            throws Exception {
        return AkkaRpcServiceUtils.remoteServiceBuilder(
                        configuration, externalAddress, externalPortRange)
                .withBindAddress(bindAddress)
                .createAndStart();
    }

    // ------------------------------------------------------------------------
    //  data client
    // ------------------------------------------------------------------------

    public ShuffleManagerClient createClient(JobID jobID) throws Exception {
        return createClient(jobID, new InstanceID());
    }

    public ShuffleManagerClient createClient(JobID jobId, InstanceID clientID) throws Exception {
        ShuffleManagerClientConfiguration clientConfiguration =
                ShuffleManagerClientConfiguration.fromConfiguration(
                        miniClusterConfiguration.getConfiguration());

        RemoteShuffleRpcService rpcService =
                shuffleMangerClientRpcServiceFactory.createRpcService();
        HaServices haServices = createHaService(miniClusterConfiguration.getConfiguration());

        ShuffleManagerClient client =
                new ShuffleManagerClientImpl(
                        jobId,
                        new ShuffleWorkerStatusListener() {
                            @Override
                            public void notifyIrrelevantWorker(InstanceID workerID) {}

                            @Override
                            public void notifyRelevantWorker(
                                    InstanceID workerID,
                                    Set<DataPartitionCoordinate> recoveredDataPartitions) {}
                        },
                        rpcService,
                        (Throwable throwable) -> new ShutDownFatalErrorHandler(),
                        clientConfiguration,
                        haServices,
                        jobHeartbeatServices,
                        clientID);
        client.start();
        return client;
    }

    // ------------------------------------------------------------------------
    //  Internal methods
    // ------------------------------------------------------------------------

    @Nonnull
    private CompletableFuture<Void> terminateRpcServices() {
        synchronized (lock) {
            final int numRpcServices = 1 + rpcServices.size();

            final Collection<CompletableFuture<?>> rpcTerminationFutures =
                    new ArrayList<>(numRpcServices);

            for (RemoteShuffleRpcService rpcService : rpcServices) {
                rpcTerminationFutures.add(rpcService.stopService());
            }

            rpcServices.clear();

            return FutureUtils.completeAll(rpcTerminationFutures);
        }
    }

    private CompletableFuture<?> terminateExecutors() {
        synchronized (lock) {
            try {
                if (ioExecutor != null) {
                    ioExecutor.shutdown();
                }
            } catch (Throwable throwable) {
                LOG.error("Failed to close the executor service.", throwable);
                return FutureUtils.completedExceptionally(throwable);
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    /** Internal factory for {@link RemoteShuffleRpcService}. */
    protected interface RpcServiceFactory {
        RemoteShuffleRpcService createRpcService() throws Exception;
    }

    /** Factory which creates and registers new {@link RemoteShuffleRpcService}. */
    protected class DedicatedRpcServiceFactory implements RpcServiceFactory {

        private final Configuration configuration;
        private final String externalAddress;
        private final String externalPortRange;
        private final String bindAddress;

        DedicatedRpcServiceFactory(
                Configuration configuration,
                String externalAddress,
                String externalPortRange,
                String bindAddress) {
            this.configuration = configuration;
            this.externalAddress = externalAddress;
            this.externalPortRange = externalPortRange;
            this.bindAddress = bindAddress;
        }

        @Override
        public RemoteShuffleRpcService createRpcService() throws Exception {
            RemoteShuffleRpcService rpcService =
                    ShuffleMiniCluster.this.createRemoteRpcService(
                            configuration, externalAddress, externalPortRange, bindAddress);

            synchronized (lock) {
                rpcServices.add(rpcService);
            }

            return rpcService;
        }
    }

    // ------------------------------------------------------------------------
    //  miscellaneous utilities
    // ------------------------------------------------------------------------

    private class TerminatingFatalErrorHandler implements FatalErrorHandler {

        private final int index;

        private TerminatingFatalErrorHandler(int index) {
            this.index = index;
        }

        @Override
        public void onFatalError(Throwable exception) {
            // first check if we are still running
            if (running) {
                LOG.error("ShuffleWorker #{} failed.", index, exception);

                synchronized (lock) {
                    shuffleWorkers.get(index).closeAsync();
                }
            }
        }
    }

    private class ShutDownFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable exception) {
            LOG.warn("Error in MiniCluster. Shutting the MiniCluster down.", exception);
            closeAsync();
        }
    }

    private class TerminatingFatalErrorHandlerFactory {

        /**
         * Create a new {@link TerminatingFatalErrorHandler} for the {@link ShuffleWorker} with the
         * given index.
         *
         * @param index into the {@link #shuffleWorkers} collection to identify the correct {@link
         *     ShuffleWorker}.
         * @return {@link TerminatingFatalErrorHandler} for the given index
         */
        @GuardedBy("lock")
        private TerminatingFatalErrorHandler create(int index) {
            return new TerminatingFatalErrorHandler(index);
        }
    }
}
