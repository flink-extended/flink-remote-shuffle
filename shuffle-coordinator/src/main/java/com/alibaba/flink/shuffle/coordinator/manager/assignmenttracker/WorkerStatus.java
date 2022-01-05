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

package com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker;

import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** The status of a shuffle worker. */
class WorkerStatus {

    private final InstanceID workerID;

    private final RegistrationID registrationID;

    private final ShuffleWorkerGateway gateway;

    private final String dataAddress;

    private final int dataPort;

    private long numHddUsableSpaceBytes;

    private long numSsdUsableSpaceBytes;

    private final Map<DataPartitionCoordinate, DataPartitionStatus> dataPartitions =
            new HashMap<>();

    public WorkerStatus(
            InstanceID workerID,
            RegistrationID registrationID,
            ShuffleWorkerGateway gateway,
            String dataAddress,
            int dataPort) {

        this.workerID = checkNotNull(workerID);
        this.registrationID = checkNotNull(registrationID);
        this.gateway = checkNotNull(gateway);
        this.dataAddress = checkNotNull(dataAddress);
        this.dataPort = dataPort;
    }

    void setNumHddUsableSpaceBytes(long numHddUsableSpaceBytes) {
        this.numHddUsableSpaceBytes = numHddUsableSpaceBytes;
    }

    void setNumSsdUsableSpaceBytes(long numSsdUsableSpaceBytes) {
        this.numSsdUsableSpaceBytes = numSsdUsableSpaceBytes;
    }

    public InstanceID getWorkerID() {
        return workerID;
    }

    public RegistrationID getRegistrationID() {
        return registrationID;
    }

    public long getNumHddUsableSpaceBytes() {
        return numHddUsableSpaceBytes;
    }

    public long getNumSsdUsableSpaceBytes() {
        return numSsdUsableSpaceBytes;
    }

    public ShuffleWorkerDescriptor createShuffleWorkerDescriptor() {
        return new ShuffleWorkerDescriptor(workerID, dataAddress, dataPort);
    }

    public void addDataPartition(DataPartitionStatus dataPartitionStatus) {
        dataPartitions.put(dataPartitionStatus.getCoordinate(), dataPartitionStatus);
    }

    public Map<DataPartitionCoordinate, DataPartitionStatus> getDataPartitions() {
        return Collections.unmodifiableMap(dataPartitions);
    }

    public void markAsReleasing(JobID jobId, DataPartitionCoordinate coordinate) {
        DataPartitionStatus dataPartitionStatus = dataPartitions.get(coordinate);

        if (dataPartitionStatus == null) {
            dataPartitionStatus = new DataPartitionStatus(jobId, coordinate, true);
            dataPartitions.put(coordinate, dataPartitionStatus);
        } else {
            dataPartitionStatus.setReleasing(true);
        }
    }

    public void removeReleasedDataPartition(DataPartitionCoordinate coordinate) {
        dataPartitions.remove(coordinate);
    }

    public ShuffleWorkerGateway getGateway() {
        return gateway;
    }

    @Override
    public String toString() {
        return "WorkerStatus{"
                + "workerID="
                + workerID
                + ", registrationID="
                + registrationID
                + ", dataAddress='"
                + dataAddress
                + '\''
                + ", dataPort="
                + dataPort
                + ", numHddUsableSpaceBytes="
                + numHddUsableSpaceBytes
                + ", numSsdUsableSpaceBytes="
                + numSsdUsableSpaceBytes
                + '}';
    }
}
