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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatListener;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatManager;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServices;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatTarget;
import com.alibaba.flink.shuffle.coordinator.heartbeat.NoOpHeartbeatManager;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderContender;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.AssignmentTracker;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.ChangedWorkerStatus;
import com.alibaba.flink.shuffle.coordinator.registration.RegistrationResponse;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleFencedRpcEndpoint;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * The ShuffleManager is responsible for: 1. Allocating shuffle workers to a FLINK job for
 * writing/reading shuffle data; 2. Managing all the ShuffleWorkers.
 */
public class ShuffleManager extends RemoteShuffleFencedRpcEndpoint<UUID>
        implements ShuffleManagerGateway, LeaderContender {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleManager.class);

    public static final String SHUFFLE_MANAGER_NAME = "shufflemanager";

    /** Unique id of the shuffle manager. */
    private final InstanceID managerID;

    /** The ha service. */
    private final HaServices haServices;

    /** The service to elect a shuffle manager leader. */
    private final LeaderElectionService leaderElectionService;

    /** Fatal error handler. */
    private final FatalErrorHandler fatalErrorHandler;

    /** The executor is responsible for some time-consuming operations. */
    private final Executor ioExecutor;

    /** The heartbeat service between the shuffle manager and jobs. */
    private final HeartbeatServices jobHeartbeatServices;

    /** The heartbeat service between the shuffle manager and shuffle works. */
    private final HeartbeatServices workerHeartbeatServices;

    /**
     * The period for shuffle manager to wait for workers to register before it starts to notify
     * clients to remove partitions.
     */
    private final long workerStatusSyncPeriodMillis;

    /**
     * The heartbeat manager is responsible for timing heartbeat between the shuffle manager and
     * jobs.
     */
    private HeartbeatManager<Void, Void> jobHeartbeatManager;

    /**
     * The heartbeat manager is responsible for timing heartbeat between the shuffle manager and
     * shuffle workers.
     */
    private HeartbeatManager<WorkerToManagerHeartbeatPayload, Void> workerHeartbeatManager;

    /** The instance id of client for each job. */
    private final Map<JobID, InstanceID> registeredClients;

    /** The component tracks the data partition assignment. */
    private final AssignmentTracker assignmentTracker;

    /** All currently registered shuffle workers. */
    private final Map<InstanceID, ShuffleWorkerRegistrationInstance> shuffleWorkers;

    /** All registering shuffle workers. */
    private final Map<InstanceID, CompletableFuture<ShuffleWorkerGateway>>
            shuffleWorkerGatewayFutures;

    private long targetWorkStatusSyncTimeMillis = 0;

    private boolean firstLeaderShipGrant = true;

    public ShuffleManager(
            RemoteShuffleRpcService rpcService,
            InstanceID managerID,
            HaServices haServices,
            FatalErrorHandler fatalErrorHandler,
            Executor ioExecutor,
            HeartbeatServices jobHeartbeatServices,
            HeartbeatServices workerHeartbeatServices,
            AssignmentTracker assignmentTracker) {
        super(rpcService, AkkaRpcServiceUtils.createRandomName(SHUFFLE_MANAGER_NAME), null);

        this.managerID = managerID;
        this.haServices = checkNotNull(haServices);
        this.leaderElectionService = haServices.createLeaderElectionService();
        this.fatalErrorHandler = fatalErrorHandler;
        this.ioExecutor = ioExecutor;

        this.jobHeartbeatServices = jobHeartbeatServices;
        this.workerHeartbeatServices = workerHeartbeatServices;
        this.jobHeartbeatManager = NoOpHeartbeatManager.getInstance();
        this.workerHeartbeatManager = NoOpHeartbeatManager.getInstance();

        this.registeredClients = new HashMap<>();
        this.assignmentTracker = assignmentTracker;

        this.workerStatusSyncPeriodMillis = 2 * workerHeartbeatServices.getHeartbeatTimeout();

        this.shuffleWorkers = new HashMap<>();
        this.shuffleWorkerGatewayFutures = new HashMap<>();
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerWorker(
            ShuffleWorkerRegistration workerRegistration) {
        CompletableFuture<ShuffleWorkerGateway> shuffleWorkerGatewayFuture =
                getRpcService()
                        .connectTo(workerRegistration.getRpcAddress(), ShuffleWorkerGateway.class);
        shuffleWorkerGatewayFutures.put(
                workerRegistration.getWorkerID(), shuffleWorkerGatewayFuture);

        LOG.info("Shuffle worker {} is registering", workerRegistration);

        return shuffleWorkerGatewayFuture.handleAsync(
                (ShuffleWorkerGateway shuffleWorkerGateway, Throwable throwable) -> {
                    final InstanceID workerID = workerRegistration.getWorkerID();
                    if (shuffleWorkerGatewayFuture == shuffleWorkerGatewayFutures.get(workerID)) {
                        shuffleWorkerGatewayFutures.remove(workerID);
                        if (throwable != null) {
                            return new RegistrationResponse.Decline(throwable.getMessage());
                        } else {
                            return registerShuffleWorkerInternal(
                                    shuffleWorkerGateway, workerRegistration);
                        }
                    } else {
                        LOG.debug(
                                "Ignoring outdated ShuffleWorkerGateway connection for {}.",
                                workerID);
                        return new RegistrationResponse.Decline(
                                "Decline outdated shuffle worker registration.");
                    }
                },
                getRpcMainThreadScheduledExecutor());
    }

    @Override
    public CompletableFuture<Acknowledge> workerReportDataPartitionReleased(
            InstanceID workerID,
            RegistrationID registrationID,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID) {
        if (!assignmentTracker.isWorkerRegistered(registrationID)) {
            LOG.warn("Received report from unmanaged Shuffle Worker {}", workerID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        assignmentTracker.workerReportDataPartitionReleased(
                registrationID, jobID, dataSetID, dataPartitionID);

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> reportDataPartitionStatus(
            InstanceID workerID,
            RegistrationID registrationID,
            List<DataPartitionStatus> dataPartitionStatuses) {
        if (!assignmentTracker.isWorkerRegistered(registrationID)) {
            LOG.warn("Received report from unmanaged Shuffle Worker {}", workerID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        dataPartitionStatuses.forEach(
                dataStatus -> {
                    JobID jobId = dataStatus.getJobId();

                    if (!assignmentTracker.isJobRegistered(jobId)) {
                        internalRegisterClient(jobId);
                    }
                });

        assignmentTracker.synchronizeWorkerDataPartitions(registrationID, dataPartitionStatuses);

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void heartbeatFromWorker(InstanceID workerID, WorkerToManagerHeartbeatPayload payload) {
        workerHeartbeatManager.receiveHeartbeat(workerID, payload);
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectWorker(InstanceID workerID, Exception cause) {
        closeShuffleWorkerConnection(workerID, cause);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerClient(
            JobID jobID, InstanceID clientID) {

        InstanceID oldInstanceId = registeredClients.get(jobID);
        if (oldInstanceId != null && !oldInstanceId.equals(clientID)) {
            LOG.warn(
                    "The job {} with instance id {} is replaced with another instance {}",
                    jobID,
                    oldInstanceId,
                    clientID);
        }
        registeredClients.put(jobID, clientID);

        internalRegisterClient(jobID);
        return CompletableFuture.completedFuture(new RegistrationSuccess(managerID));
    }

    private void internalRegisterClient(JobID jobId) {
        if (!assignmentTracker.isJobRegistered(jobId)) {
            LOG.info("Registering session {}", jobId);

            assignmentTracker.registerJob(jobId);

            jobHeartbeatManager.monitorTarget(
                    instanceIDFromJobID(jobId),
                    new HeartbeatTarget<Void>() {
                        @Override
                        public void receiveHeartbeat(
                                InstanceID heartbeatOrigin, Void heartbeatPayload) {
                            // We do not need to notify session side
                        }

                        @Override
                        public void requestHeartbeat(
                                InstanceID requestOrigin, Void heartbeatPayload) {
                            // should not do it
                        }
                    });
        }
    }

    @Override
    public CompletableFuture<Acknowledge> unregisterClient(JobID jobID, InstanceID clientID) {
        try {
            checkInstanceIdConsistent(jobID, clientID, "Unregister client");
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }

        LOG.info("Client {} unregister actively, will do nothing now", jobID);
        registeredClients.remove(jobID);

        // Do nothing here since the job might recover.
        // Let's trust the timeout to ensure the final cleanup.

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    private void unregisterClientInternal(JobID jobId) {
        if (assignmentTracker.isJobRegistered(jobId)) {
            LOG.info("Unregister session {}", jobId);
            assignmentTracker.unregisterJob(jobId);
            jobHeartbeatManager.unmonitorTarget(instanceIDFromJobID(jobId));
        }
    }

    @Override
    public CompletableFuture<ManagerToJobHeartbeatPayload> heartbeatFromClient(
            JobID jobID, InstanceID clientID, Set<InstanceID> cachedWorkerList) {
        try {
            checkInstanceIdConsistent(jobID, clientID, "heartbeatFromClient");
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }

        jobHeartbeatManager.receiveHeartbeat(instanceIDFromJobID(jobID), null);

        ChangedWorkerStatus changedWorkerStatus =
                assignmentTracker.computeChangedWorkers(
                        jobID,
                        cachedWorkerList,
                        System.nanoTime() / 1000000 >= targetWorkStatusSyncTimeMillis);
        return CompletableFuture.completedFuture(
                new ManagerToJobHeartbeatPayload(this.managerID, changedWorkerStatus));
    }

    @Override
    public CompletableFuture<ShuffleResource> requestShuffleResource(
            JobID jobID,
            InstanceID clientID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            String dataPartitionFactoryName) {

        try {
            checkInstanceIdConsistent(jobID, clientID, "Allocate shuffle resource");
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }

        LOG.info(
                "Request resource for session {}, dataset {}, producer {} and total consumer {}",
                jobID,
                dataSetID,
                mapPartitionID,
                numberOfConsumers);

        try {
            ShuffleResource allocatedShuffleResource =
                    assignmentTracker.requestShuffleResource(
                            jobID,
                            dataSetID,
                            mapPartitionID,
                            numberOfConsumers,
                            dataPartitionFactoryName);
            return CompletableFuture.completedFuture(allocatedShuffleResource);
        } catch (Exception e) {
            LOG.error("Request new Shuffle Resource failed.", e);
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> releaseShuffleResource(
            JobID jobID, InstanceID clientID, DataSetID dataSetID, MapPartitionID mapPartitionID) {

        try {
            checkInstanceIdConsistent(jobID, clientID, "Release shuffle resource");
        } catch (Exception e) {
            return FutureUtils.completedExceptionally(e);
        }

        LOG.info(
                "Remove resource for session {}, dataset {}, producer {}",
                jobID,
                dataSetID,
                mapPartitionID);
        assignmentTracker.releaseShuffleResource(jobID, dataSetID, mapPartitionID);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredWorkers() {
        return CompletableFuture.supplyAsync(
                shuffleWorkers::size, getRpcMainThreadScheduledExecutor());
    }

    @Override
    public CompletableFuture<Map<InstanceID, ShuffleWorkerMetrics>> getShuffleWorkerMetrics() {

        List<CompletableFuture<Pair<InstanceID, ShuffleWorkerMetrics>>> workerMetrics =
                shuffleWorkers.values().stream()
                        .map(
                                worker ->
                                        worker.shuffleWorkerGateway
                                                .getWorkerMetrics()
                                                .thenApply(
                                                        metric ->
                                                                Pair.of(
                                                                        worker.shuffleWorkerId,
                                                                        metric)))
                        .collect(Collectors.toList());

        return FutureUtils.combineAll(workerMetrics)
                .thenApply(
                        pairs ->
                                pairs.stream()
                                        .collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
    }

    @Override
    public CompletableFuture<List<JobID>> listJobs() {
        return CompletableFuture.completedFuture(assignmentTracker.listJobs());
    }

    @Override
    public CompletableFuture<JobDataPartitionDistribution> getJobDataPartitionDistribution(
            JobID jobID) {
        Map<DataPartitionCoordinate, ShuffleWorkerRegistration> jobDataPartitionDistribution =
                assignmentTracker.getDataPartitionDistribution(jobID).entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> shuffleWorkers.get(e.getValue()).getRegistration()));
        return CompletableFuture.completedFuture(
                new JobDataPartitionDistribution(jobDataPartitionDistribution));
    }

    private void checkInstanceIdConsistent(
            JobID jobID, InstanceID requestId, String loggedOperation) {
        InstanceID oldInstanceId = registeredClients.get(jobID);
        checkState(
                Objects.equals(oldInstanceId, requestId),
                String.format(
                        "%s requests with inconsistent instance id for job %s, current id is %s and requested is %s",
                        loggedOperation, jobID, oldInstanceId, requestId));
    }

    // ------------------------------------------------------------------------
    //  RPC lifecycle methods
    // ------------------------------------------------------------------------

    @Override
    protected void onStart() throws Exception {
        try {
            startShuffleManagerServices();
        } catch (Throwable t) {
            final Exception exception =
                    new Exception(String.format("Could not start %s", getAddress()), t);
            onFatalError(exception);
            throw exception;
        }
    }

    private void startShuffleManagerServices() throws Exception {
        try {
            leaderElectionService.start(this);
        } catch (Exception e) {
            handleStartShuffleManagerServicesException(e);
        }
    }

    private void handleStartShuffleManagerServicesException(Exception e) throws Exception {
        try {
            stopShuffleManagerServices();
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }

        throw e;
    }

    @Override
    public final CompletableFuture<Void> onStop() {
        try {
            stopShuffleManagerServices();
        } catch (Exception exception) {
            return FutureUtils.completedExceptionally(
                    new ShuffleException(
                            "Could not properly shut down the ShuffleManager.", exception));
        }

        return CompletableFuture.completedFuture(null);
    }

    private void stopShuffleManagerServices() throws Exception {
        Exception exception = null;

        stopHeartbeatServices();

        try {
            leaderElectionService.stop();
        } catch (Exception e) {
            exception = e;
        }

        try {
            haServices.closeAndCleanupAllData();
        } catch (Exception e) {
            exception = exception == null ? e : exception;
        }

        clearStateInternal();

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Error Handling
    // ------------------------------------------------------------------------

    /**
     * Notifies the ShuffleManager that a fatal error has occurred and it cannot proceed.
     *
     * @param t The exception describing the fatal error
     */
    protected void onFatalError(Throwable t) {
        try {
            LOG.error("Fatal error occurred in ShuffleManager.", t);
        } catch (Throwable ignored) {
        }

        // The fatal error handler implementation should make sure that this call is non-blocking
        fatalErrorHandler.onFatalError(t);
    }

    // ------------------------------------------------------------------------
    //  Leader Contender
    // ------------------------------------------------------------------------

    /**
     * Callback method when current ShuffleManager is granted leadership.
     *
     * @param newLeaderSessionID unique leadershipID
     */
    @Override
    public void grantLeadership(final UUID newLeaderSessionID) {
        final CompletableFuture<Boolean> acceptLeadershipFuture =
                CompletableFuture.supplyAsync(
                        () -> tryAcceptLeadership(newLeaderSessionID),
                        getUnfencedMainThreadExecutor());

        final CompletableFuture<Void> confirmationFuture =
                acceptLeadershipFuture.thenAcceptAsync(
                        (acceptLeadership) -> {
                            if (acceptLeadership) {
                                // confirming the leader session ID might be blocking,
                                leaderElectionService.confirmLeadership(
                                        new LeaderInformation(newLeaderSessionID, getAddress()));
                            }
                        },
                        ioExecutor);

        confirmationFuture.whenCompleteAsync(
                (Void ignored, Throwable throwable) -> {
                    if (throwable != null) {
                        onFatalError(throwable);
                    }
                    targetWorkStatusSyncTimeMillis =
                            System.nanoTime() / 1000000 + workerStatusSyncPeriodMillis;
                },
                getUnfencedMainThreadExecutor());
    }

    @Override
    public void revokeLeadership() {
        runAsyncWithoutFencing(
                () -> {
                    LOG.info(
                            "ShuffleManager {} was revoked leadership. Clearing fencing token {}.",
                            getAddress(),
                            getFencingToken());

                    clearStateInternal();

                    setFencingToken(null);

                    // We force increase this deadline to avoid there are long time period between
                    // the revoke and re-grant.
                    targetWorkStatusSyncTimeMillis =
                            System.nanoTime() / 1000000 + Integer.MAX_VALUE;
                });
    }

    @Override
    public void handleError(Throwable throwable) {
        onFatalError(throwable);
    }

    private boolean tryAcceptLeadership(UUID newLeaderSessionID) {
        if (leaderElectionService.hasLeadership(newLeaderSessionID)) {
            LOG.info(
                    "ShuffleManager {} was granted leadership with fencing token {}",
                    getAddress(),
                    newLeaderSessionID);

            // clear the state if we've been the leader before
            if (getFencingToken() != null) {
                clearStateInternal();
            }

            setFencingToken(newLeaderSessionID);

            startHeartbeatServices();

            firstLeaderShipGrant = false;

            return true;
        }

        return false;
    }

    // ------------------------------------------------------------------------
    //  Heartbeat Service
    // ------------------------------------------------------------------------
    private void startHeartbeatServices() {
        if (firstLeaderShipGrant) {
            LOG.info("Initialize the heartbeat services.");
            jobHeartbeatManager =
                    jobHeartbeatServices.createHeartbeatManager(
                            managerID,
                            new JobHeartbeatListener(),
                            getRpcMainThreadScheduledExecutor(),
                            log);

            workerHeartbeatManager =
                    workerHeartbeatServices.createHeartbeatManagerSender(
                            managerID,
                            new WorkerHeartbeatListener(),
                            getRpcMainThreadScheduledExecutor(),
                            log);
        }
    }

    private void stopHeartbeatServices() {
        jobHeartbeatManager.stop();
        workerHeartbeatManager.stop();
    }

    // ------------------------------------------------------------------------
    //  Clear the internal state
    // ------------------------------------------------------------------------

    private void clearStateInternal() {
        LOG.info("Currently, we would not clear the state to avoid large-scale restarting.");
    }

    // ------------------------------------------------------------------------
    //  ShuffleWorker related action
    // ------------------------------------------------------------------------

    /**
     * Registers a new ShuffleWorker.
     *
     * @param shuffleWorkerRegistration shuffle worker registration parameters
     * @return RegistrationResponse
     */
    private RegistrationResponse registerShuffleWorkerInternal(
            ShuffleWorkerGateway shuffleWorkerGateway,
            ShuffleWorkerRegistration shuffleWorkerRegistration) {

        final InstanceID workerID = shuffleWorkerRegistration.getWorkerID();
        final ShuffleWorkerRegistrationInstance oldRegistration = shuffleWorkers.remove(workerID);
        if (oldRegistration != null) {
            // TODO :: suggest old ShuffleWorker to stop itself
            log.info(
                    "Replacing old registration of ShuffleWorker {}: {}.",
                    workerID,
                    oldRegistration.getShuffleWorkerRegisterId());

            // remove old shuffle worker registration from assignment tracker.
            assignmentTracker.unregisterWorker(oldRegistration.getShuffleWorkerRegisterId());
        }

        final String workerAddress = shuffleWorkerRegistration.getHostname();
        ShuffleWorkerRegistrationInstance newRecord =
                new ShuffleWorkerRegistrationInstance(
                        workerID, shuffleWorkerGateway, shuffleWorkerRegistration);

        log.info(
                "Registering ShuffleWorker with ID {} ({}, {}) at ShuffleManager",
                workerID,
                workerAddress,
                newRecord.getShuffleWorkerRegisterId());
        shuffleWorkers.put(workerID, newRecord);

        workerHeartbeatManager.monitorTarget(
                workerID,
                new HeartbeatTarget<Void>() {
                    @Override
                    public void receiveHeartbeat(InstanceID instanceID, Void payload) {
                        // the ShuffleManager will always send heartbeat requests to the
                        // ShuffleWorker
                    }

                    @Override
                    public void requestHeartbeat(InstanceID instanceID, Void payload) {
                        shuffleWorkerGateway.heartbeatFromManager(instanceID);
                    }
                });

        assignmentTracker.registerWorker(
                workerID,
                newRecord.getShuffleWorkerRegisterId(),
                shuffleWorkerGateway,
                shuffleWorkerRegistration.getHostname(),
                shuffleWorkerRegistration.getDataPort());

        LOG.info(
                "Stat on register worker: shuffleWorkers.size = {}, assignmentTracker has tracked {} workers",
                shuffleWorkers.size(),
                assignmentTracker.getNumberOfWorkers());

        return new ShuffleWorkerRegistrationSuccess(
                newRecord.getShuffleWorkerRegisterId(), managerID);
    }

    private void closeShuffleWorkerConnection(
            final InstanceID shuffleWorkerId, final Exception cause) {
        LOG.info("Disconnect ShuffleWorker {} because: {}", shuffleWorkerId, cause.getMessage());

        workerHeartbeatManager.unmonitorTarget(shuffleWorkerId);

        final ShuffleWorkerRegistrationInstance record = shuffleWorkers.remove(shuffleWorkerId);
        if (record != null) {
            assignmentTracker.unregisterWorker(record.getShuffleWorkerRegisterId());

            record.getShuffleWorkerGateway().disconnectManager(cause);
        } else {
            log.info(
                    "No open ShuffleWorker connection {}. Ignoring close ShuffleWorker connection. Closing reason was: {}",
                    shuffleWorkerId,
                    cause.getMessage());
        }

        LOG.info(
                "Stat on unregister worker: shuffleWorkers.size = {}, assignmentTracker has tracked {} workers",
                shuffleWorkers.size(),
                assignmentTracker.getNumberOfWorkers());
    }

    AssignmentTracker getAssignmentTracker() {
        return assignmentTracker;
    }

    public Map<JobID, InstanceID> getRegisteredClients() {
        return registeredClients;
    }

    public Map<InstanceID, ShuffleWorkerRegistrationInstance> getShuffleWorkers() {
        return shuffleWorkers;
    }

    HeartbeatManager<Void, Void> getJobHeartbeatManager() {
        return jobHeartbeatManager;
    }

    HeartbeatManager<WorkerToManagerHeartbeatPayload, Void> getWorkerHeartbeatManager() {
        return workerHeartbeatManager;
    }

    // ------------------------------------------------------------------------
    //  Static utility classes
    // ------------------------------------------------------------------------
    private class JobHeartbeatListener implements HeartbeatListener<Void, Void> {

        @Override
        public void notifyHeartbeatTimeout(InstanceID instanceID) {
            validateRunsInMainThread();
            LOG.info("The heartbeat of client with id {} timed out.", instanceID);

            unregisterClientInternal(jobIdFromInstanceID(instanceID));
        }

        @Override
        public void reportPayload(InstanceID instanceID, Void payload) {
            validateRunsInMainThread();
        }

        @Override
        public Void retrievePayload(InstanceID instanceID) {
            return null;
        }
    }

    private class WorkerHeartbeatListener
            implements HeartbeatListener<WorkerToManagerHeartbeatPayload, Void> {

        @Override
        public void notifyHeartbeatTimeout(InstanceID instanceID) {
            validateRunsInMainThread();
            LOG.info("The heartbeat of shuffle worker with id {} timed out.", instanceID);

            closeShuffleWorkerConnection(
                    instanceID,
                    new TimeoutException(
                            "The heartbeat of ShuffleWorker with id "
                                    + instanceID
                                    + "  timed out."));
        }

        @Override
        public void reportPayload(InstanceID instanceID, WorkerToManagerHeartbeatPayload payload) {
            final ShuffleWorkerRegistrationInstance shuffleWorkerRegistrationInstance =
                    shuffleWorkers.get(instanceID);
            if (shuffleWorkerRegistrationInstance != null) {
                reportDataPartitionStatus(
                        instanceID,
                        shuffleWorkerRegistrationInstance.getShuffleWorkerRegisterId(),
                        payload.getDataPartitionStatuses());
                assignmentTracker.reportWorkerStorageSpaces(
                        instanceID,
                        shuffleWorkers.get(instanceID).getShuffleWorkerRegisterId(),
                        payload.getNumHddUsableBytes(),
                        payload.getNumSsdUsableBytes());
            } else {
                LOG.warn(
                        "The shuffle worker with id {} is not registered before but receive the heartbeat.",
                        instanceID);
            }
        }

        @Override
        public Void retrievePayload(InstanceID instanceID) {
            return null;
        }
    }

    /** This class records a shuffle worker's registration in one success attempt. */
    private static class ShuffleWorkerRegistrationInstance {

        private final InstanceID shuffleWorkerId;

        private final RegistrationID shuffleWorkerRegisterId;

        private final ShuffleWorkerGateway shuffleWorkerGateway;

        private final ShuffleWorkerRegistration registration;

        public ShuffleWorkerRegistrationInstance(
                InstanceID shuffleWorkerId,
                ShuffleWorkerGateway shuffleWorkerGateway,
                ShuffleWorkerRegistration registration) {
            this.shuffleWorkerId = shuffleWorkerId;
            this.registration = registration;
            this.shuffleWorkerRegisterId = new RegistrationID();
            this.shuffleWorkerGateway = shuffleWorkerGateway;
        }

        public RegistrationID getShuffleWorkerRegisterId() {
            return shuffleWorkerRegisterId;
        }

        public ShuffleWorkerGateway getShuffleWorkerGateway() {
            return shuffleWorkerGateway;
        }

        public InstanceID getShuffleWorkerId() {
            return shuffleWorkerId;
        }

        public ShuffleWorkerRegistration getRegistration() {
            return registration;
        }
    }

    private static InstanceID instanceIDFromJobID(JobID jobId) {
        return new InstanceID(jobId.getId());
    }

    private static JobID jobIdFromInstanceID(InstanceID instanceID) {
        return new JobID(instanceID.getId());
    }
}
