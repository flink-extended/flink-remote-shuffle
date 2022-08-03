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

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** {@link ShuffleDescriptor} for the flink remote shuffle. */
public class RemoteShuffleDescriptor implements ShuffleDescriptor {

    private static final long serialVersionUID = -6333647241057311308L;

    /** The ID of the upstream tasks' result partition. */
    private final ResultPartitionID resultPartitionID;

    /** The ID of the containing job. */
    private final JobID jobId;

    /** The allocated shuffle resource. */
    private final ShuffleResource shuffleResource;

    private final int numMaps;

    private final boolean isMapPartition;

    public RemoteShuffleDescriptor(
            ResultPartitionID resultPartitionID,
            JobID jobId,
            ShuffleResource shuffleResource,
            boolean isMapPartition,
            int numMaps) {
        this.resultPartitionID = resultPartitionID;
        this.jobId = jobId;
        this.shuffleResource = shuffleResource;
        this.isMapPartition = isMapPartition;
        this.numMaps = numMaps;
        checkState(isMapPartition || numMaps > 0);
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

    public boolean isMapPartition() {
        return isMapPartition;
    }

    public int getNumMaps() {
        return numMaps;
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
                + ", isMapPartition="
                + isMapPartition
                + '}';
    }
}
