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

import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.storage.UsableStorageSpaceInfo;

import java.io.Serializable;
import java.util.Map;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Information provided by the ShuffleWorker when it registers to the ShuffleManager. */
public class ShuffleWorkerRegistration implements Serializable {

    private static final long serialVersionUID = 5666500078135011993L;

    /** The rpc address of the ShuffleWorker that registers. */
    private final String rpcAddress;

    /** The hostname of the ShuffleWorker that registers. */
    private final String hostname;

    /** The resource id of the ShuffleWorker that registers. */
    private final InstanceID workerID;

    /** The port used for data transfer. */
    private final int dataPort;

    /** The process id of the shuffle worker. Currently, it is only used in e2e tests. */
    private final int processID;

    /** Available storage space information by partition factory. */
    private final Map<String, UsableStorageSpaceInfo> usableSpace;

    public ShuffleWorkerRegistration(
            final String rpcAddress,
            final String hostname,
            final InstanceID workerID,
            final int dataPort,
            int processID,
            Map<String, UsableStorageSpaceInfo> usableSpace) {
        this.rpcAddress = checkNotNull(rpcAddress);
        this.hostname = checkNotNull(hostname);
        this.workerID = checkNotNull(workerID);
        this.dataPort = dataPort;
        this.processID = processID;
        this.usableSpace = checkNotNull(usableSpace);
    }

    public String getRpcAddress() {
        return rpcAddress;
    }

    public String getHostname() {
        return hostname;
    }

    public InstanceID getWorkerID() {
        return workerID;
    }

    public int getDataPort() {
        return dataPort;
    }

    public int getProcessID() {
        return processID;
    }

    public Map<String, UsableStorageSpaceInfo> getUsableStorageSpace() {
        return usableSpace;
    }

    @Override
    public String toString() {
        return "ShuffleWorkerRegistration{"
                + "rpcAddress='"
                + rpcAddress
                + '\''
                + ", hostname='"
                + hostname
                + '\''
                + ", instanceID="
                + workerID
                + ", dataPort="
                + dataPort
                + ", processId="
                + processID
                + '}';
    }
}
