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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.manager.JobDataPartitionDistribution;
import com.alibaba.flink.shuffle.coordinator.manager.ManagerToJobHeartbeatPayload;
import com.alibaba.flink.shuffle.coordinator.manager.RegistrationSuccess;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManagerGateway;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerRegistration;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerRegistrationSuccess;
import com.alibaba.flink.shuffle.coordinator.manager.WorkerToManagerHeartbeatPayload;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.ChangedWorkerStatus;
import com.alibaba.flink.shuffle.coordinator.registration.RegistrationResponse;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;
import com.alibaba.flink.shuffle.utils.QuadFunction;
import com.alibaba.flink.shuffle.utils.TriFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/** A testing {@link ShuffleManagerGateway} implementation. */
public class TestingShuffleManagerGateway implements ShuffleManagerGateway {

    private final String address;

    private final String hostname;

    private final UUID shuffleManagerId;

    private final InstanceID instanceID;

    private volatile Function<ShuffleWorkerRegistration, CompletableFuture<RegistrationResponse>>
            registerShuffleWorkerConsumer;

    private volatile BiFunction<
                    Pair<InstanceID, RegistrationID>,
                    Triple<JobID, DataSetID, DataPartitionID>,
                    CompletableFuture<Acknowledge>>
            workerReportDataPartitionReleasedConsumer;

    private volatile TriFunction<
                    InstanceID,
                    RegistrationID,
                    List<DataPartitionStatus>,
                    CompletableFuture<Acknowledge>>
            reportShuffleDataStatusConsumer;

    private BiConsumer<InstanceID, WorkerToManagerHeartbeatPayload>
            heartbeatFromShuffleWorkerConsumer;

    private BiFunction<InstanceID, Exception, CompletableFuture<Acknowledge>>
            disconnectShuffleWorkerConsumer;

    private Function<JobID, CompletableFuture<RegistrationResponse>> registerClientConsumer;

    private Function<JobID, CompletableFuture<Acknowledge>> unregisterClientConsumer;

    private BiFunction<JobID, Set<InstanceID>, CompletableFuture<ManagerToJobHeartbeatPayload>>
            heartbeatFromClientConsumer;

    private QuadFunction<
                    JobID, DataSetID, MapPartitionID, Integer, CompletableFuture<ShuffleResource>>
            allocateShuffleResourceConsumer;

    private TriFunction<JobID, DataSetID, MapPartitionID, CompletableFuture<Acknowledge>>
            releaseShuffleResourceConsumer;

    public TestingShuffleManagerGateway() {
        this("localhost/" + UUID.randomUUID(), "localhost", UUID.randomUUID(), new InstanceID());
    }

    public TestingShuffleManagerGateway(
            String address, String hostname, UUID shuffleManagerId, InstanceID instanceID) {
        this.address = address;
        this.hostname = hostname;
        this.shuffleManagerId = shuffleManagerId;
        this.instanceID = instanceID;
    }

    // ------------------------------ setters ------------------------------------------

    public void setRegisterShuffleWorkerConsumer(
            Function<ShuffleWorkerRegistration, CompletableFuture<RegistrationResponse>>
                    registerShuffleWorkerConsumer) {
        this.registerShuffleWorkerConsumer = registerShuffleWorkerConsumer;
    }

    public void setWorkerReportDataPartitionReleasedConsumer(
            BiFunction<
                            Pair<InstanceID, RegistrationID>,
                            Triple<JobID, DataSetID, DataPartitionID>,
                            CompletableFuture<Acknowledge>>
                    workerReportDataPartitionReleasedConsumer) {
        this.workerReportDataPartitionReleasedConsumer = workerReportDataPartitionReleasedConsumer;
    }

    public void setReportShuffleDataStatusConsumer(
            TriFunction<
                            InstanceID,
                            RegistrationID,
                            List<DataPartitionStatus>,
                            CompletableFuture<Acknowledge>>
                    reportShuffleDataStatusConsumer) {
        this.reportShuffleDataStatusConsumer = reportShuffleDataStatusConsumer;
    }

    public void setHeartbeatFromShuffleWorkerConsumer(
            BiConsumer<InstanceID, WorkerToManagerHeartbeatPayload>
                    heartbeatFromShuffleWorkerConsumer) {
        this.heartbeatFromShuffleWorkerConsumer = heartbeatFromShuffleWorkerConsumer;
    }

    public void setDisconnectShuffleWorkerConsumer(
            BiFunction<InstanceID, Exception, CompletableFuture<Acknowledge>>
                    disconnectShuffleWorkerConsumer) {
        this.disconnectShuffleWorkerConsumer = disconnectShuffleWorkerConsumer;
    }

    public void setRegisterClientConsumer(
            Function<JobID, CompletableFuture<RegistrationResponse>> registerClientConsumer) {
        this.registerClientConsumer = registerClientConsumer;
    }

    public void setUnregisterClientConsumer(
            Function<JobID, CompletableFuture<Acknowledge>> unregisterClientConsumer) {
        this.unregisterClientConsumer = unregisterClientConsumer;
    }

    public void setHeartbeatFromClientConsumer(
            BiFunction<JobID, Set<InstanceID>, CompletableFuture<ManagerToJobHeartbeatPayload>>
                    heartbeatFromClientConsumer) {
        this.heartbeatFromClientConsumer = heartbeatFromClientConsumer;
    }

    public void setAllocateShuffleResourceConsumer(
            QuadFunction<
                            JobID,
                            DataSetID,
                            MapPartitionID,
                            Integer,
                            CompletableFuture<ShuffleResource>>
                    allocateShuffleResourceConsumer) {
        this.allocateShuffleResourceConsumer = allocateShuffleResourceConsumer;
    }

    public void setReleaseShuffleResourceConsumer(
            TriFunction<JobID, DataSetID, MapPartitionID, CompletableFuture<Acknowledge>>
                    releaseShuffleResourceConsumer) {
        this.releaseShuffleResourceConsumer = releaseShuffleResourceConsumer;
    }

    // ------------------------------ shuffle worker gateway ---------------------------

    @Override
    public CompletableFuture<RegistrationResponse> registerWorker(
            ShuffleWorkerRegistration workerRegistration) {
        final Function<ShuffleWorkerRegistration, CompletableFuture<RegistrationResponse>>
                currentConsumer = registerShuffleWorkerConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(workerRegistration);
        }

        return CompletableFuture.completedFuture(
                new ShuffleWorkerRegistrationSuccess(new RegistrationID(), instanceID));
    }

    @Override
    public CompletableFuture<Acknowledge> workerReportDataPartitionReleased(
            InstanceID workerID,
            RegistrationID registrationID,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID) {
        final BiFunction<
                        Pair<InstanceID, RegistrationID>,
                        Triple<JobID, DataSetID, DataPartitionID>,
                        CompletableFuture<Acknowledge>>
                currentConsumer = workerReportDataPartitionReleasedConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(
                    Pair.of(workerID, registrationID),
                    Triple.of(jobID, dataSetID, dataPartitionID));
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Acknowledge> reportDataPartitionStatus(
            InstanceID workerID,
            RegistrationID registrationID,
            List<DataPartitionStatus> dataPartitionStatuses) {
        final TriFunction<
                        InstanceID,
                        RegistrationID,
                        List<DataPartitionStatus>,
                        CompletableFuture<Acknowledge>>
                currentConsumer = reportShuffleDataStatusConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(workerID, registrationID, dataPartitionStatuses);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void heartbeatFromWorker(InstanceID workerID, WorkerToManagerHeartbeatPayload payload) {
        BiConsumer<InstanceID, WorkerToManagerHeartbeatPayload> currentConsumer =
                heartbeatFromShuffleWorkerConsumer;
        if (currentConsumer != null) {
            currentConsumer.accept(workerID, payload);
        }
    }

    @Override
    public CompletableFuture<Acknowledge> disconnectWorker(InstanceID workerID, Exception cause) {
        BiFunction<InstanceID, Exception, CompletableFuture<Acknowledge>> currentConsumer =
                disconnectShuffleWorkerConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(workerID, cause);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<RegistrationResponse> registerClient(
            JobID jobID, InstanceID clientID) {
        Function<JobID, CompletableFuture<RegistrationResponse>> currentConsumer =
                registerClientConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(jobID);
        }

        return CompletableFuture.completedFuture(new RegistrationSuccess(getInstanceID()));
    }

    @Override
    public CompletableFuture<Acknowledge> unregisterClient(JobID jobID, InstanceID clientID) {
        Function<JobID, CompletableFuture<Acknowledge>> currentConsumer = unregisterClientConsumer;
        if (currentConsumer != null) {
            return unregisterClientConsumer.apply(jobID);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<ManagerToJobHeartbeatPayload> heartbeatFromClient(
            JobID jobID, InstanceID clientID, Set<InstanceID> cachedWorkerList) {
        BiFunction<JobID, Set<InstanceID>, CompletableFuture<ManagerToJobHeartbeatPayload>>
                currentConsumer = heartbeatFromClientConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(jobID, cachedWorkerList);
        }

        return CompletableFuture.completedFuture(
                new ManagerToJobHeartbeatPayload(
                        clientID,
                        new ChangedWorkerStatus(Collections.emptyList(), Collections.emptyMap())));
    }

    @Override
    public CompletableFuture<ShuffleResource> requestShuffleResource(
            JobID jobID,
            InstanceID clientID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            String dataPartitionFactoryName) {
        return requestShuffleResource(
                jobID,
                clientID,
                dataSetID,
                mapPartitionID,
                numberOfConsumers,
                dataPartitionFactoryName,
                null);
    }

    @Override
    public CompletableFuture<ShuffleResource> requestShuffleResource(
            JobID jobID,
            InstanceID clientID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            String dataPartitionFactoryName,
            String taskLocation) {
        QuadFunction<JobID, DataSetID, MapPartitionID, Integer, CompletableFuture<ShuffleResource>>
                currentConsumer = allocateShuffleResourceConsumer;
        if (currentConsumer != null) {
            return currentConsumer.apply(jobID, dataSetID, mapPartitionID, numberOfConsumers);
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Acknowledge> releaseShuffleResource(
            JobID jobID, InstanceID clientID, DataSetID dataSetID, MapPartitionID mapPartitionID) {
        TriFunction<JobID, DataSetID, MapPartitionID, CompletableFuture<Acknowledge>>
                currentConsumer = releaseShuffleResourceConsumer;
        if (currentConsumer != null) {
            currentConsumer.apply(jobID, dataSetID, mapPartitionID);
        }

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public CompletableFuture<Integer> getNumberOfRegisteredWorkers() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Map<InstanceID, ShuffleWorkerMetrics>> getShuffleWorkerMetrics() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<JobID>> listJobs() {
        throw new UnsupportedOperationException("Not supported currently.");
    }

    @Override
    public CompletableFuture<JobDataPartitionDistribution> getJobDataPartitionDistribution(
            JobID jobID) {
        throw new UnsupportedOperationException("Not supported currently.");
    }

    public InstanceID getInstanceID() {
        return instanceID;
    }

    @Override
    public UUID getFencingToken() {
        return shuffleManagerId;
    }

    @Override
    public String getAddress() {
        return address;
    }

    @Override
    public String getHostname() {
        return hostname;
    }
}
