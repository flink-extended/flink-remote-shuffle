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
import com.alibaba.flink.shuffle.core.ids.InstanceID;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** The status of workers that get changed compared with the cached worker list. */
public class ChangedWorkerStatus implements Serializable {

    private static final long serialVersionUID = -3808740665897798476L;

    /** The workers get unavailable. */
    private final List<InstanceID> irrelevantWorkers;

    /** The workers get available again. */
    private final Map<InstanceID, Set<DataPartitionCoordinate>> relevantWorkers;

    public ChangedWorkerStatus(
            List<InstanceID> irrelevantWorkers,
            Map<InstanceID, Set<DataPartitionCoordinate>> relevantWorkers) {
        this.irrelevantWorkers = checkNotNull(irrelevantWorkers);
        this.relevantWorkers = checkNotNull(relevantWorkers);
    }

    public List<InstanceID> getIrrelevantWorkers() {
        return irrelevantWorkers;
    }

    public Map<InstanceID, Set<DataPartitionCoordinate>> getRelevantWorkers() {
        return relevantWorkers;
    }
}
