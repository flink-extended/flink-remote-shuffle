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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.core.ids.InstanceID;

import java.io.Serializable;
import java.util.Objects;

/** Describes the address and identification of a shuffle worker. */
public class ShuffleWorkerDescriptor implements Serializable {

    private static final long serialVersionUID = 7675147692379529718L;

    private final InstanceID workerId;

    private final String workerAddress;

    private final int dataPort;

    public ShuffleWorkerDescriptor(InstanceID workerId, String workerAddress, int dataPort) {
        this.workerId = workerId;
        this.workerAddress = workerAddress;
        this.dataPort = dataPort;
    }

    public InstanceID getWorkerId() {
        return workerId;
    }

    public String getWorkerAddress() {
        return workerAddress;
    }

    public int getDataPort() {
        return dataPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ShuffleWorkerDescriptor that = (ShuffleWorkerDescriptor) o;
        return dataPort == that.dataPort
                && Objects.equals(workerId, that.workerId)
                && Objects.equals(workerAddress, that.workerAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workerId, workerAddress, dataPort);
    }

    @Override
    public String toString() {
        return workerId + "@[" + workerAddress + ":" + dataPort + "]";
    }
}
