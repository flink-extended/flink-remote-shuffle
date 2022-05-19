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

package com.alibaba.flink.shuffle.client;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.common.utils.SingleThreadExecutorValidator;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatListener;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManager;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatTarget;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalListener;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.manager.JobDataPartitionDistribution;
import com.alibaba.flink.shuffle.coordinator.manager.ManagerToJobHeartbeatPayload;
import com.alibaba.flink.shuffle.coordinator.manager.RegistrationSuccess;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManagerJobGateway;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.registration.ConnectingConnection;
import com.alibaba.flink.shuffle.coordinator.registration.EstablishedConnection;
import com.alibaba.flink.shuffle.coordinator.registration.RegistrationConnectionListener;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.RpcTargetAddress;
import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * The client that is responsible for interacting with ShuffleManager to request and release
 * resources.
 */
public class ShuffleManagerClientImpl implements ShuffleManagerClient, LeaderRetrievalListener {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerClientImpl.class);

    private final JobID jobID;

    private final InstanceID clientID;

    private final ShuffleWorkerStatusListener shuffleWorkerStatusListener;

    private final RemoteShuffleRpcService rpcService;

    private final FatalErrorHandler fatalErrorHandler;

    private final ShuffleManagerClientConfiguration shuffleManagerClientConfiguration;

    // ------------------------------------------------------------------------

    private final ScheduledExecutorService mainThreadExecutor;

    private final SingleThreadExecutorValidator mainThreadExecutorValidator;

    private final HeartbeatManager<Void, Void> shuffleManagerHeartbeatManager;

    private final LeaderRetrievalService shuffleManagerLeaderRetrieveService;

    @Nullable private RpcTargetAddress shuffleManagerAddress;

    @Nullable
    private ConnectingConnection<ShuffleManagerJobGateway, RegistrationSuccess>
            shuffleManagerConnection;

    @Nullable
    private EstablishedConnection<ShuffleManagerJobGateway, RegistrationSuccess>
            establishedConnection;

    @Nullable private UUID currentRegistrationTimeoutId;

    @Nullable private CompletableFuture<Void> connectionFuture;

    /**
     * The list of cached shuffle workers. Here we could not directly use the list cached in the
     * partition tracker since the list must be consistent with the result of the last heartbeat.
     * However, after the last heartbeat, the PartitionTracker might have tracked more shuffle
     * workers. If there are restarted shuffle workers, the next heartbeat would ignore these
     * shuffle workers.
     */
    private final Set<InstanceID> relatedShuffleWorkers = new HashSet<>();

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public ShuffleManagerClientImpl(
            JobID jobID,
            ShuffleWorkerStatusListener shuffleWorkerStatusListener,
            RemoteShuffleRpcService rpcService,
            FatalErrorHandler fatalErrorHandler,
            ShuffleManagerClientConfiguration shuffleManagerClientConfiguration,
            HaServices haServices,
            HeartbeatServices heartbeatService,
            InstanceID clientID) {

        this.jobID = checkNotNull(jobID);
        this.clientID = checkNotNull(clientID);
        this.shuffleWorkerStatusListener = checkNotNull(shuffleWorkerStatusListener);
        this.rpcService = checkNotNull(rpcService);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.shuffleManagerClientConfiguration = checkNotNull(shuffleManagerClientConfiguration);

        mainThreadExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        r -> {
                            Thread thread = new Thread(r);
                            thread.setName("shuffle-client-" + jobID);
                            return thread;
                        });
        mainThreadExecutorValidator = new SingleThreadExecutorValidator(mainThreadExecutor);

        this.shuffleManagerLeaderRetrieveService =
                checkNotNull(haServices)
                        .createLeaderRetrievalService(HaServices.LeaderReceptor.SHUFFLE_CLIENT);

        shuffleManagerHeartbeatManager =
                heartbeatService.createHeartbeatManagerSender(
                        new InstanceID(jobID.getId()),
                        new ManagerHeartbeatListener(),
                        new ScheduledExecutorServiceAdapter(mainThreadExecutor),
                        LOG);
    }

    public ShuffleManagerClientImpl(
            JobID jobID,
            ShuffleWorkerStatusListener shuffleWorkerStatusListener,
            RemoteShuffleRpcService rpcService,
            FatalErrorHandler fatalErrorHandler,
            ShuffleManagerClientConfiguration shuffleManagerClientConfiguration,
            HaServices haServices,
            HeartbeatServices heartbeatService) {
        this(
                jobID,
                shuffleWorkerStatusListener,
                rpcService,
                fatalErrorHandler,
                shuffleManagerClientConfiguration,
                haServices,
                heartbeatService,
                new InstanceID());
    }

    // ------------------------------------------------------------------------
    //  Life Cycle
    // ------------------------------------------------------------------------

    @Override
    public void start() {
        try {
            connectionFuture = new CompletableFuture<>();
            shuffleManagerLeaderRetrieveService.start(this);

            // Wait till the connection is established
            connectionFuture.get();
        } catch (Exception e) {
            fatalErrorHandler.onFatalError(e);
        }
    }

    @Override
    public void synchronizeWorkerStatus(Set<InstanceID> initialWorkers) throws Exception {
        // Force a synchronous heartbeat to the shuffle manager
        LOG.info("Synchronize worker status with the manager.");

        CompletableFuture<ManagerToJobHeartbeatPayload> payloadFuture = new CompletableFuture<>();
        mainThreadExecutor.submit(
                () -> {
                    // Here we directly merge the two arrays. The only
                    // result might be unnecessary unrelated notification,
                    // which should not cause problems.
                    relatedShuffleWorkers.addAll(initialWorkers);
                    return emitHeartbeat()
                            .whenCompleteAsync(
                                    (payload, future) -> {
                                        updateClientStatusOnHeartbeatSuccess(payload, future);
                                        payloadFuture.complete(payload);
                                    },
                                    mainThreadExecutor);
                });

        ManagerToJobHeartbeatPayload heartbeatPayload =
                payloadFuture.get(
                        shuffleManagerClientConfiguration.getRpcTimeout(), TimeUnit.MILLISECONDS);
        updatePartitionTrackerOnHeartbeatSuccess(heartbeatPayload, null);
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            return;
        }
        try {
            mainThreadExecutor
                    .submit(
                            () -> {
                                if (establishedConnection != null) {
                                    shuffleManagerHeartbeatManager.unmonitorTarget(
                                            establishedConnection.getResponse().getInstanceID());

                                    if (establishedConnection != null) {
                                        establishedConnection
                                                .getGateway()
                                                .unregisterClient(jobID, clientID);
                                    }
                                }
                            })
                    .get();
        } catch (Exception e) {
            LOG.error("Failed to close the established connections", e);
        }

        mainThreadExecutor.shutdownNow();
    }

    // ------------------------------------------------------------------------
    //  Internal shuffle manager connection methods
    // ------------------------------------------------------------------------

    @Override
    public void notifyLeaderAddress(LeaderInformation leaderInfo) {
        try {
            mainThreadExecutor.execute(() -> notifyOfNewShuffleManagerLeader(leaderInfo));
        } catch (Throwable throwable) {
            if (isClosed.get()) {
                return;
            }
            throw throwable;
        }
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(
                new Exception("Failed to retrieve shuffle manager address", exception));
    }

    private void notifyOfNewShuffleManagerLeader(LeaderInformation leaderInfo) {
        shuffleManagerAddress = createShuffleManagerAddress(leaderInfo);
        reconnectToShuffleManager(
                new ShuffleException(
                        String.format(
                                "ShuffleManager leader changed to new address %s",
                                shuffleManagerAddress)));
    }

    @Nullable
    private RpcTargetAddress createShuffleManagerAddress(LeaderInformation leaderInfo) {
        if (leaderInfo == LeaderInformation.empty()) {
            return null;
        }
        return new RpcTargetAddress(leaderInfo.getLeaderAddress(), leaderInfo.getLeaderSessionID());
    }

    private void reconnectToShuffleManager(Exception cause) {
        closeShuffleManagerConnection(cause);
        startRegistrationTimeout();
        tryConnectToShuffleManager();
    }

    private void tryConnectToShuffleManager() {
        if (shuffleManagerAddress != null) {
            connectToShuffleManager();
        }
    }

    private void connectToShuffleManager() {
        assert (shuffleManagerAddress != null);
        assert (establishedConnection == null);
        assert (shuffleManagerConnection == null);

        LOG.info("Connecting to ShuffleManager {}.", shuffleManagerAddress);

        shuffleManagerConnection =
                new ConnectingConnection<>(
                        LOG,
                        "ShuffleManager",
                        ShuffleManagerJobGateway.class,
                        rpcService,
                        shuffleManagerClientConfiguration.getRetryingRegistrationConfiguration(),
                        shuffleManagerAddress.getTargetAddress(),
                        shuffleManagerAddress.getLeaderUUID(),
                        mainThreadExecutor,
                        new ShuffleManagerRegistrationListener(),
                        (gateway) -> gateway.registerClient(jobID, clientID));

        shuffleManagerConnection.start();
    }

    private void establishShuffleManagerConnection(
            ShuffleManagerJobGateway shuffleManagerGateway, RegistrationSuccess response) {

        // monitor the shuffle manager as heartbeat target
        shuffleManagerHeartbeatManager.monitorTarget(
                response.getInstanceID(),
                new HeartbeatTarget<Void>() {
                    @Override
                    public void receiveHeartbeat(InstanceID instanceID, Void heartbeatPayload) {
                        // Will never call this
                    }

                    @Override
                    public void requestHeartbeat(InstanceID instanceID, Void heartbeatPayload) {
                        heartbeatToShuffleManager();
                    }
                });

        establishedConnection = new EstablishedConnection<>(shuffleManagerGateway, response);

        stopRegistrationTimeout();

        checkState(connectionFuture != null);
        connectionFuture.complete(null);
    }

    private void closeShuffleManagerConnection(Exception cause) {
        if (establishedConnection != null) {
            final InstanceID shuffleManagerInstanceID =
                    establishedConnection.getResponse().getInstanceID();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Close ShuffleManager connection {}.", shuffleManagerInstanceID, cause);
            } else {
                LOG.info("Close ShuffleManager connection {}.", shuffleManagerInstanceID);
            }
            shuffleManagerHeartbeatManager.unmonitorTarget(shuffleManagerInstanceID);

            ShuffleManagerJobGateway shuffleManagerGateway = establishedConnection.getGateway();
            shuffleManagerGateway.unregisterClient(jobID, clientID);

            establishedConnection = null;
        }

        if (shuffleManagerConnection != null) {
            if (!shuffleManagerConnection.isConnected()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Terminating registration attempts towards ShuffleManager {}.",
                            shuffleManagerConnection.getTargetAddress(),
                            cause);
                } else {
                    LOG.info(
                            "Terminating registration attempts towards ShuffleManager {}.",
                            shuffleManagerConnection.getTargetAddress());
                }
            }

            shuffleManagerConnection.close();
            shuffleManagerConnection = null;
        }
    }

    private void startRegistrationTimeout() {
        final UUID newRegistrationTimeoutId = UUID.randomUUID();
        currentRegistrationTimeoutId = newRegistrationTimeoutId;

        mainThreadExecutor.schedule(
                () -> registrationTimeout(newRegistrationTimeoutId),
                shuffleManagerClientConfiguration.getMaxRegistrationDuration(),
                TimeUnit.MILLISECONDS);
    }

    private void stopRegistrationTimeout() {
        currentRegistrationTimeoutId = null;
    }

    private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
        if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
            fatalErrorHandler.onFatalError(
                    new Exception(
                            String.format(
                                    "Could not register at the ShuffleManager within the specified maximum "
                                            + "registration duration %s. This indicates a problem with this instance. Terminating now.",
                                    shuffleManagerClientConfiguration
                                            .getMaxRegistrationDuration())));
        }
    }

    public void heartbeatToShuffleManager() {
        emitHeartbeat()
                .whenCompleteAsync(
                        (payload, exception) -> {
                            updateClientStatusOnHeartbeatSuccess(payload, exception);
                            updatePartitionTrackerOnHeartbeatSuccess(payload, exception);
                        },
                        mainThreadExecutor);
    }

    private CompletableFuture<ManagerToJobHeartbeatPayload> emitHeartbeat() {
        if (establishedConnection != null) {
            return establishedConnection
                    .getGateway()
                    .heartbeatFromClient(jobID, clientID, relatedShuffleWorkers);
        }

        return FutureUtils.completedExceptionally(
                new RuntimeException("Connection not established yet"));
    }

    /**
     * Updates the shuffle client status on heartbeat success. This method must be called in the
     * {@code mainExecutor}.
     */
    private void updateClientStatusOnHeartbeatSuccess(
            ManagerToJobHeartbeatPayload payload, Throwable exception) {
        mainThreadExecutorValidator.assertRunningInTargetThread();

        if (exception != null) {
            LOG.warn("Heartbeat to ShuffleManager failed.", exception);
            return;
        }

        shuffleManagerHeartbeatManager.receiveHeartbeat(payload.getManagerID(), null);

        payload.getJobChangedWorkerStatus()
                .getIrrelevantWorkers()
                .forEach(relatedShuffleWorkers::remove);

        payload.getJobChangedWorkerStatus()
                .getRelevantWorkers()
                .forEach((workerId, ignored) -> relatedShuffleWorkers.add(workerId));
    }

    /** Updates the partition tracker status on heartbeat success. */
    private void updatePartitionTrackerOnHeartbeatSuccess(
            ManagerToJobHeartbeatPayload payload, Throwable exception) {
        if (exception != null) {
            LOG.warn("Heartbeat to ShuffleManager failed.", exception);
            return;
        }

        for (InstanceID unavailable : payload.getJobChangedWorkerStatus().getIrrelevantWorkers()) {
            LOG.info("Got unrelated shuffle worker: " + unavailable);

            shuffleWorkerStatusListener.notifyIrrelevantWorker(unavailable);
        }

        payload.getJobChangedWorkerStatus()
                .getRelevantWorkers()
                .forEach(
                        (workerId, partitions) -> {
                            LOG.info("Got newly related shuffle worker: " + workerId);

                            shuffleWorkerStatusListener.notifyRelevantWorker(workerId, partitions);
                        });
    }

    // ------------------------------------------------------------------------
    //  Manage the shuffle partitions
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<ShuffleResource> requestShuffleResource(
            DataSetID dataSetId,
            MapPartitionID mapPartitionId,
            int numberOfSubpartitions,
            String dataPartitionFactoryName) {
        return sendRpcCall(
                (shuffleManagerJobGateway) ->
                        shuffleManagerJobGateway.requestShuffleResource(
                                jobID,
                                clientID,
                                dataSetId,
                                mapPartitionId,
                                numberOfSubpartitions,
                                dataPartitionFactoryName));
    }

    @Override
    public CompletableFuture<ShuffleResource> requestShuffleResource(
            DataSetID dataSetId,
            MapPartitionID mapPartitionId,
            int numberOfSubpartitions,
            String dataPartitionFactoryName,
            String taskLocation) {
        return sendRpcCall(
                (shuffleManagerJobGateway) ->
                        shuffleManagerJobGateway.requestShuffleResource(
                                jobID,
                                clientID,
                                dataSetId,
                                mapPartitionId,
                                numberOfSubpartitions,
                                dataPartitionFactoryName,
                                taskLocation));
    }

    @Override
    public void releaseShuffleResource(DataSetID dataSetId, MapPartitionID mapPartitionId) {
        sendRpcCall(
                shuffleManagerJobGateway ->
                        shuffleManagerJobGateway.releaseShuffleResource(
                                jobID, clientID, dataSetId, mapPartitionId));
    }

    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredWorkers() {
        return sendRpcCall(ShuffleManagerJobGateway::getNumberOfRegisteredWorkers);
    }

    @Override
    public CompletableFuture<Map<InstanceID, ShuffleWorkerMetrics>> getShuffleWorkerMetrics() {
        return sendRpcCall(ShuffleManagerJobGateway::getShuffleWorkerMetrics);
    }

    @Override
    public CompletableFuture<List<JobID>> listJobs(boolean includeMyself) {
        return sendRpcCall(
                shuffleManagerJobGateway ->
                        shuffleManagerJobGateway
                                .listJobs()
                                .thenApply(
                                        jobIds -> {
                                            if (includeMyself) {
                                                return jobIds;
                                            }

                                            return jobIds.stream()
                                                    .filter(id -> !id.equals(jobID))
                                                    .collect(Collectors.toList());
                                        }));
    }

    @Override
    public CompletableFuture<JobDataPartitionDistribution> getJobDataPartitionDistribution(
            JobID jobID) {
        return sendRpcCall(
                shuffleManagerJobGateway ->
                        shuffleManagerJobGateway.getJobDataPartitionDistribution(jobID));
    }

    private <T> CompletableFuture<T> sendRpcCall(
            Function<ShuffleManagerJobGateway, CompletableFuture<T>> rpcCallFunction) {

        return CompletableFuture.supplyAsync(
                        () -> {
                            if (establishedConnection == null) {
                                Exception e = new Exception("No connection to the shuffle manager");
                                LOG.warn(
                                        "No connection to the shuffle manager, abort the request",
                                        e);
                                throw new CompletionException(e);
                            }

                            return establishedConnection.getGateway();
                        },
                        mainThreadExecutor)
                .thenComposeAsync(rpcCallFunction, mainThreadExecutor);
    }

    // ------------------------------------------------------------------------
    //  Static utility classes
    // ------------------------------------------------------------------------

    private final class ShuffleManagerRegistrationListener
            implements RegistrationConnectionListener<
                    ConnectingConnection<ShuffleManagerJobGateway, RegistrationSuccess>,
                    RegistrationSuccess> {

        @Override
        public void onRegistrationSuccess(
                ConnectingConnection<ShuffleManagerJobGateway, RegistrationSuccess> connection,
                RegistrationSuccess success) {
            final ShuffleManagerJobGateway shuffleManagerGateway = connection.getTargetGateway();

            mainThreadExecutor.execute(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (shuffleManagerConnection == connection) {
                            try {
                                establishShuffleManagerConnection(shuffleManagerGateway, success);
                            } catch (Throwable t) {
                                LOG.error(
                                        "Establishing Shuffle Manager connection in client failed",
                                        t);
                            }
                        }
                    });
        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            fatalErrorHandler.onFatalError(failure);
        }
    }

    private class ManagerHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(InstanceID instanceID) {
            LOG.info("Timeout with remote shuffle manager {}", instanceID);
            if (establishedConnection != null
                    && establishedConnection.getResponse().getInstanceID().equals(instanceID)) {
                reconnectToShuffleManager(new Exception("Heartbeat timeout"));
            }
        }

        @Override
        public void reportPayload(InstanceID instanceID, Void payload) {}

        @Override
        public Void retrievePayload(InstanceID instanceID) {
            return null;
        }
    }
}
