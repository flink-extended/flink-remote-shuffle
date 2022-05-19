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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Payload for heartbeats sent from the ShuffleWorker to the ShuffleManager. */
public class WorkerToManagerHeartbeatPayload implements Serializable {

    private static final long serialVersionUID = 5801527514715010018L;

    private final List<DataPartitionStatus> dataPartitionStatuses;

    private final Map<String, StorageSpaceInfo> storageSpaceInfos;

    public WorkerToManagerHeartbeatPayload(
            List<DataPartitionStatus> dataPartitionStatuses,
            Map<String, StorageSpaceInfo> storageSpaceInfos) {
        this.dataPartitionStatuses = dataPartitionStatuses;
        this.storageSpaceInfos = checkNotNull(storageSpaceInfos);
    }

    public List<DataPartitionStatus> getDataPartitionStatuses() {
        return dataPartitionStatuses;
    }

    public Map<String, StorageSpaceInfo> getStorageSpaceInfos() {
        return storageSpaceInfos;
    }
}
