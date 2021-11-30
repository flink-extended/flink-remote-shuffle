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

package com.alibaba.flink.shuffle.plugin;

import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.plugin.utils.IdMappingUtils;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.util.Optional;

/** {@link ShuffleDescriptor} for the flink remote shuffle. */
public class RemoteShuffleDescriptor implements ShuffleDescriptor {

    private static final long serialVersionUID = -6333647241057311308L;

    /** The ID of the upstream tasks' result partition. */
    private final ResultPartitionID resultPartitionID;

    /** The ID of the containing job. */
    private final JobID jobId;

    /** The allocated shuffle resource. */
    private final ShuffleResource shuffleResource;

    public RemoteShuffleDescriptor(
            ResultPartitionID resultPartitionID, JobID jobId, ShuffleResource shuffleResource) {
        this.resultPartitionID = resultPartitionID;
        this.jobId = jobId;
        this.shuffleResource = shuffleResource;
    }

    @Override
    public Optional<ResourceID> storesLocalResourcesOn() {
        return Optional.empty();
    }

    @Override
    public ResultPartitionID getResultPartitionID() {
        return resultPartitionID;
    }

    public JobID getJobId() {
        return jobId;
    }

    public DataSetID getDataSetId() {
        return IdMappingUtils.fromFlinkDataSetId(
                resultPartitionID.getPartitionId().getIntermediateDataSetID());
    }

    public DataPartitionID getDataPartitionID() {
        return IdMappingUtils.fromFlinkResultPartitionID(resultPartitionID);
    }

    public ShuffleResource getShuffleResource() {
        return shuffleResource;
    }

    @Override
    public String toString() {
        return "RemoteShuffleDescriptor{"
                + "resultPartitionID="
                + resultPartitionID
                + ", jobId="
                + jobId
                + ", dataSetId="
                + getDataSetId()
                + ", dataPartitionID="
                + getDataPartitionID()
                + ", shuffleResource="
                + shuffleResource
                + '}';
    }
}
