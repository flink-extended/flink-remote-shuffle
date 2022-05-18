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

package com.alibaba.flink.shuffle.coordinator.worker;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatListener;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManager;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatTarget;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalListener;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManagerGateway;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerRegistration;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerRegistrationSuccess;
import com.alibaba.flink.shuffle.coordinator.manager.WorkerToManagerHeartbeatPayload;
import com.alibaba.flink.shuffle.coordinator.registration.ConnectingConnection;
import com.alibaba.flink.shuffle.coordinator.registration.EstablishedConnection;
import com.alibaba.flink.shuffle.coordinator.registration.RegistrationConnectionListener;
import com.alibaba.flink.shuffle.coordinator.worker.checker.ShuffleWorkerChecker;
import com.alibaba.flink.shuffle.coordinator.worker.metastore.Metastore;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcEndpoint;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.RpcTargetAddress;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;
import com.alibaba.flink.shuffle.transfer.NettyServer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static com.alibaba.flink.shuffle.common.utils.ProcessUtils.getProcessID;

/** The worker that actually manages the data partitions. */
public class ShuffleWorker extends RemoteShuffleRpcEndpoint implements ShuffleWorkerGateway {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleWorker.class);

    private static final String SHUFFLE_WORKER_NAME = "shuffleworker";

    private final ShuffleWorkerConfiguration shuffleWorkerConfiguration;

    private final ShuffleWorkerLocation shuffleWorkerLocation;

    private final ShuffleWorkerChecker workerChecker;

    /** The fatal error handler to use in case of a fatal error. */
    private final FatalErrorHandler fatalErrorHandler;

    private final Metastore metaStore;

    private final PartitionedDataStore dataStore;

    private final NettyServer nettyServer;

    // ------------------------------------------------------------------------

    private final HeartbeatManager<Void, WorkerToManagerHeartbeatPayload> heartbeatManager;

    private final LeaderRetrievalService leaderRetrieveService;

    @Nullable private RpcTargetAddress shuffleManagerAddress;

    @Nullable
    private ConnectingConnection<ShuffleManagerGateway, ShuffleWorkerRegistrationSuccess>
            connectingConnection;

    @Nullable
    private EstablishedConnection<ShuffleManagerGateway, ShuffleWorkerRegistrationSuccess>
            establishedConnection;

    @Nullable private UUID currentRegistrationTimeoutId;

    protected ShuffleWorker(
            RemoteShuffleRpcService rpcService,
            ShuffleWorkerConfiguration shuffleWorkerConfiguration,
            HaServices haServices,
            HeartbeatServices heartbeatServices,
            FatalErrorHandler fatalErrorHandler,
            ShuffleWorkerLocation shuffleWorkerLocation,
            Metastore metaStore,
            PartitionedDataStore dataStore,
            ShuffleWorkerChecker workerChecker,
            NettyServer nettyServer) {

        super(rpcService, AkkaRpcServiceUtils.createRandomName(SHUFFLE_WORKER_NAME));

        this.shuffleWorkerConfiguration = checkNotNull(shuffleWorkerConfiguration);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

        this.shuffleWorkerLocation = checkNotNull(shuffleWorkerLocation);

        this.metaStore = checkNotNull(metaStore);
        metaStore.setPartitionRemovedConsumer(this::onPartitionRemoved);

        this.dataStore = checkNotNull(dataStore);
        this.workerChecker = checkNotNull(workerChecker);
        this.nettyServer = checkNotNull(nettyServer);

        this.leaderRetrieveService =
                haServices.createLeaderRetrievalService(HaServices.LeaderReceptor.SHUFFLE_WORKER);

        this.heartbeatManager =
                heartbeatServices.createHeartbeatManager(
                        shuffleWorkerLocation.getWorkerID(),
                        new ManagerHeartbeatListener(),
                        getRpcMainThreadScheduledExecutor(),
                        log);
    }

    // ------------------------------------------------------------------------
    //  Life Cycle
    // ------------------------------------------------------------------------

    @Override
    protected void onStart() throws Exception {
        try {
            leaderRetrieveService.start(new ShuffleManagerLeaderListener());
        } catch (Exception e) {
            handleStartShuffleWorkerServicesException(e);
        }
    }

    private void handleStartShuffleWorkerServicesException(Exception e) throws Exception {
        try {
            stopShuffleWorkerServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    private void stopShuffleWorkerServices() throws Exception {
        Exception exception = null;

        try {
            dataStore.shutDown(false);
        } catch (Exception e) {
            exception = e;
        }

        try {
            metaStore.close();
        } catch (Exception e) {
            exception = exception == null ? e : exception;
        }

        try {
            workerChecker.close();
        } catch (Exception e) {
            exception = exception == null ? e : exception;
        }

        try {
            nettyServer.shutdown();
        } catch (Exception e) {
            exception = exception == null ? e : exception;
        }

        try {
            heartbeatManager.stop();
        } catch (Exception e) {
            exception = exception == null ? e : exception;
        }

        try {
            leaderRetrieveService.stop();
        } catch (Exception e) {
            exception = exception == null ? e : exception;
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    /** Called to shut down the ShuffleWorker. The method closes all ShuffleWorker services. */
    @Override
    public CompletableFuture<Void> onStop() {
        LOG.info("Stopping ShuffleWorker {}.", getAddress());

        ShuffleException cause = new ShuffleException("The ShuffleWorker is shutting down.");

        closeShuffleManagerConnection(cause);

        LOG.info("Stopped ShuffleWorker {}.", getAddress());

        try {
            stopShuffleWorkerServices();
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            LOG.warn("Failed to stop shuffle worker services", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public void heartbeatFromManager(InstanceID managerID) {
        heartbeatManager.requestHeartbeat(managerID, null);
    }

    // ------------------------------------------------------------------------
    //  Internal shuffle manager connection methods
    // ------------------------------------------------------------------------

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
        connectToShuffleManager();
    }

    private void connectToShuffleManager() {
        if (shuffleManagerAddress == null) {
            return;
        }

        checkState(establishedConnection == null, "Must be null.");
        checkState(connectingConnection == null, "Must be null.");

        LOG.info("Connecting to ShuffleManager {}.", shuffleManagerAddress);

        connectingConnection =
                new ConnectingConnection<>(
                        LOG,
                        "ShuffleManager",
                        ShuffleManagerGateway.class,
                        getRpcService(),
                        shuffleWorkerConfiguration.getRetryingRegistrationConfiguration(),
                        shuffleManagerAddress.getTargetAddress(),
                        shuffleManagerAddress.getLeaderUUID(),
                        getRpcMainThreadScheduledExecutor(),
                        new ShuffleManagerRegistrationListener(),
                        (gateway) ->
                                gateway.registerWorker(
                                        new ShuffleWorkerRegistration(
                                                getAddress(),
                                                getRpcService().getAddress(),
                                                shuffleWorkerLocation.getWorkerID(),
                                                shuffleWorkerLocation.getDataPort(),
                                                getProcessID(),
                                                dataStore.getStorageSpaceInfos())));

        connectingConnection.start();
    }

    private void establishShuffleManagerConnection(
            ShuffleManagerGateway shuffleManagerGateway,
            ShuffleWorkerRegistrationSuccess response) {

        CompletableFuture<Acknowledge> shuffleDataStatusReportResponseFuture;
        try {
            shuffleDataStatusReportResponseFuture =
                    shuffleManagerGateway.reportDataPartitionStatus(
                            shuffleWorkerLocation.getWorkerID(),
                            response.getRegistrationID(),
                            metaStore.listDataPartitions());

            shuffleDataStatusReportResponseFuture.whenCompleteAsync(
                    (acknowledge, throwable) -> {
                        if (throwable != null) {
                            reconnectToShuffleManager(
                                    new Exception(
                                            "Failed to send initial shuffle data status report to shuffle manager.",
                                            throwable));
                        }
                    },
                    getRpcMainThreadScheduledExecutor());
        } catch (Exception e) {
            LOG.warn("Initial shuffle data partition status report failed", e);
        }

        // monitor the shuffle manager as heartbeat target
        heartbeatManager.monitorTarget(
                response.getInstanceID(),
                new HeartbeatTarget<WorkerToManagerHeartbeatPayload>() {
                    @Override
                    public void receiveHeartbeat(
                            InstanceID instanceID,
                            WorkerToManagerHeartbeatPayload heartbeatPayload) {
                        shuffleManagerGateway.heartbeatFromWorker(instanceID, heartbeatPayload);
                    }

                    @Override
                    public void requestHeartbeat(
                            InstanceID instanceID,
                            WorkerToManagerHeartbeatPayload heartbeatPayload) {
                        // the ShuffleWorker won't send heartbeat requests to the ShuffleManager
                    }
                });

        establishedConnection = new EstablishedConnection<>(shuffleManagerGateway, response);

        stopRegistrationTimeout();
    }

    private void closeShuffleManagerConnection(Exception cause) {
        if (establishedConnection != null) {
            final InstanceID shuffleManagerInstanceID =
                    establishedConnection.getResponse().getInstanceID();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Close ShuffleManager connection {}.", shuffleManagerInstanceID, cause);
            } else {
                LOG.error("Close ShuffleManager connection " + shuffleManagerInstanceID, cause);
            }
            heartbeatManager.unmonitorTarget(shuffleManagerInstanceID);

            ShuffleManagerGateway shuffleManagerGateway = establishedConnection.getGateway();
            shuffleManagerGateway.disconnectWorker(shuffleWorkerLocation.getWorkerID(), cause);

            establishedConnection = null;
        }

        if (connectingConnection != null) {
            if (!connectingConnection.isConnected()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(
                            "Terminating registration attempts towards ShuffleManager {}.",
                            connectingConnection.getTargetAddress(),
                            cause);
                } else {
                    LOG.info(
                            "Terminating registration attempts towards ShuffleManager {}.",
                            connectingConnection.getTargetAddress());
                }
            }

            connectingConnection.close();
            connectingConnection = null;
        }
    }

    private void startRegistrationTimeout() {
        final long maxRegistrationDuration =
                shuffleWorkerConfiguration.getMaxRegistrationDuration();

        final UUID newRegistrationTimeoutId = UUID.randomUUID();
        currentRegistrationTimeoutId = newRegistrationTimeoutId;
        scheduleRunAsync(
                () -> registrationTimeout(newRegistrationTimeoutId),
                maxRegistrationDuration,
                TimeUnit.MILLISECONDS);
    }

    private void stopRegistrationTimeout() {
        currentRegistrationTimeoutId = null;
    }

    private void registrationTimeout(@Nonnull UUID registrationTimeoutId) {
        if (registrationTimeoutId.equals(currentRegistrationTimeoutId)) {
            onFatalError(
                    new Exception(
                            String.format(
                                    "Could not register at the ShuffleManager within the specified maximum "
                                            + "registration duration %s. This indicates a problem with this instance. Terminating now.",
                                    shuffleWorkerConfiguration.getMaxRegistrationDuration())));
        }
    }

    // ----------------------------------------------------------------------
    // Disconnection RPCs
    // ----------------------------------------------------------------------

    @Override
    public void disconnectManager(Exception cause) {
        if (isRunning()) {
            reconnectToShuffleManager(cause);
        }
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the ShuffleWorker that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    void onFatalError(final Throwable t) {
        try {
            LOG.error("Fatal error occurred in ShuffleWorker {}.", getAddress(), t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    // ------------------------------------------------------------------------
    //  RPC methods
    // ------------------------------------------------------------------------

    private void onPartitionRemoved(JobID jobID, DataPartitionCoordinate coordinate) {
        runAsync(
                () -> {
                    if (establishedConnection != null) {
                        establishedConnection
                                .getGateway()
                                .workerReportDataPartitionReleased(
                                        shuffleWorkerLocation.getWorkerID(),
                                        establishedConnection.getResponse().getRegistrationID(),
                                        jobID,
                                        coordinate.getDataSetId(),
                                        coordinate.getDataPartitionId());
                    } else {
                        LOG.warn(
                                "No connection to the shuffle manager and cannot notify of the partition {}-{} removed",
                                jobID,
                                coordinate);
                    }
                });
    }

    @Override
    public CompletableFuture<Acknowledge> releaseDataPartition(
            JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
        // DataStore will notify metastore after removing the data files
        dataStore.releaseDataPartition(dataSetID, dataPartitionID, null);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> removeReleasedDataPartitionMeta(
            JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
        metaStore.removeReleasingDataPartition(
                new DataPartitionCoordinate(dataSetID, dataPartitionID));
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<ShuffleWorkerMetrics> getWorkerMetrics() {
        ShuffleWorkerMetrics workerMetrics = new ShuffleWorkerMetrics();
        workerMetrics.setMetric(
                ShuffleWorkerMetricKeys.AVAILABLE_READING_BUFFERS_KEY,
                dataStore.getReadingBufferDispatcher().numAvailableBuffers());
        workerMetrics.setMetric(
                ShuffleWorkerMetricKeys.AVAILABLE_WRITING_BUFFERS_KEY,
                dataStore.getWritingBufferDispatcher().numAvailableBuffers());
        workerMetrics.setMetric(
                ShuffleWorkerMetricKeys.DATA_PARTITION_NUMBERS_KEY, metaStore.getSize());
        return CompletableFuture.completedFuture(workerMetrics);
    }

    // ------------------------------------------------------------------------
    //  Static utility classes
    // ------------------------------------------------------------------------

    private class ShuffleManagerLeaderListener implements LeaderRetrievalListener {

        @Override
        public void notifyLeaderAddress(LeaderInformation leaderInfo) {
            runAsync(() -> notifyOfNewShuffleManagerLeader(leaderInfo));
        }

        @Override
        public void handleError(Exception exception) {
            fatalErrorHandler.onFatalError(
                    new Exception("Failed to retrieve shuffle manager address", exception));
        }
    }

    private final class ShuffleManagerRegistrationListener
            implements RegistrationConnectionListener<
                    ConnectingConnection<ShuffleManagerGateway, ShuffleWorkerRegistrationSuccess>,
                    ShuffleWorkerRegistrationSuccess> {

        @Override
        public void onRegistrationSuccess(
                ConnectingConnection<ShuffleManagerGateway, ShuffleWorkerRegistrationSuccess>
                        connection,
                ShuffleWorkerRegistrationSuccess success) {
            final ShuffleManagerGateway shuffleManagerGateway = connection.getTargetGateway();

            runAsync(
                    () -> {
                        // filter out outdated connections
                        //noinspection ObjectEquality
                        if (connectingConnection == connection) {
                            try {
                                establishShuffleManagerConnection(shuffleManagerGateway, success);
                            } catch (Throwable t) {
                                LOG.error(
                                        "Establishing ShuffleManager connection in ShuffleWorker failed",
                                        t);
                            }
                        }
                    });
        }

        @Override
        public void onRegistrationFailure(Throwable failure) {
            onFatalError(failure);
        }
    }

    private class ManagerHeartbeatListener
            implements HeartbeatListener<Void, WorkerToManagerHeartbeatPayload> {

        @Override
        public void notifyHeartbeatTimeout(InstanceID instanceID) {
            validateRunsInMainThread();

            // first check whether the timeout is still valid
            if (establishedConnection != null
                    && establishedConnection.getResponse().getInstanceID().equals(instanceID)) {
                LOG.info("The heartbeat of ShuffleManager with id {} timed out.", instanceID);

                reconnectToShuffleManager(
                        new Exception(
                                String.format(
                                        "The heartbeat of ShuffleManager with id %s timed out.",
                                        instanceID)));
            } else {
                LOG.debug(
                        "Received heartbeat timeout for outdated ShuffleManager id {}. Ignoring the timeout.",
                        instanceID);
            }
        }

        @Override
        public void reportPayload(InstanceID instanceID, Void payload) {
            // nothing to do since the payload is of type Void
        }

        @Override
        public WorkerToManagerHeartbeatPayload retrievePayload(InstanceID instanceID) {
            validateRunsInMainThread();
            try {
                return new WorkerToManagerHeartbeatPayload(
                        metaStore.listDataPartitions(), dataStore.getStorageSpaceInfos());
            } catch (Exception e) {
                return new WorkerToManagerHeartbeatPayload(
                        new ArrayList<>(), dataStore.getStorageSpaceInfos());
            }
        }
    }
}
