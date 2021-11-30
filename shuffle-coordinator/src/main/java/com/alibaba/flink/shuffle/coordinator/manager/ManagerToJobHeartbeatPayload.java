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

import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.ChangedWorkerStatus;
import com.alibaba.flink.shuffle.core.ids.InstanceID;

import java.io.Serializable;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** The payload for the heartbeat between shuffle manager and job. */
public class ManagerToJobHeartbeatPayload implements Serializable {

    private static final long serialVersionUID = 7306522388105576112L;

    /** The resource id of the shuffle manager. */
    private final InstanceID managerID;

    /** The workers that get changed for this job. */
    private final ChangedWorkerStatus changedWorkerStatus;

    public ManagerToJobHeartbeatPayload(
            InstanceID managerID, ChangedWorkerStatus changedWorkerStatus) {
        this.managerID = checkNotNull(managerID);
        this.changedWorkerStatus = checkNotNull(changedWorkerStatus);
    }

    public InstanceID getManagerID() {
        return managerID;
    }

    public ChangedWorkerStatus getJobChangedWorkerStatus() {
        return changedWorkerStatus;
    }
}
