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

import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DiskType;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** Shuffle Resource representation for the data partition. */
public class DefaultShuffleResource implements ShuffleResource {

    private static final long serialVersionUID = -8562771913795553025L;

    /** The addresses of the allocated shuffle resource for the data partition. */
    private final ShuffleWorkerDescriptor[] shuffleWorkerDescriptors;

    /** The type of the data partition. */
    private final DataPartition.DataPartitionType dataPartitionType;

    /** The preferred disk type of the {@link ShuffleResource} . */
    private final DiskType diskType;

    /** The ID of the consumed partition group. */
    private long consumerGroupID;

    public DefaultShuffleResource(
            ShuffleWorkerDescriptor[] shuffleWorkerDescriptors,
            DataPartition.DataPartitionType dataPartitionType,
            DiskType diskType) {
        checkArgument(shuffleWorkerDescriptors.length > 0, "Must be positive.");
        checkArgument(
                dataPartitionType == DataPartition.DataPartitionType.REDUCE_PARTITION
                        || shuffleWorkerDescriptors.length == 1,
                "Illegal number of shuffle worker descriptors.");
        checkArgument(diskType != null, "Must not be null.");

        this.shuffleWorkerDescriptors = shuffleWorkerDescriptors;
        this.dataPartitionType = dataPartitionType;
        this.diskType = diskType;
    }

    @Override
    public ShuffleWorkerDescriptor[] getReducePartitionLocations() {
        checkState(dataPartitionType.equals(DataPartition.DataPartitionType.REDUCE_PARTITION));
        return shuffleWorkerDescriptors;
    }

    @Override
    public ShuffleWorkerDescriptor getMapPartitionLocation() {
        checkState(dataPartitionType.equals(DataPartition.DataPartitionType.MAP_PARTITION));
        return shuffleWorkerDescriptors[0];
    }

    @Override
    public void setConsumerGroupID(long consumerGroupID) {
        this.consumerGroupID = consumerGroupID;
    }

    @Override
    public long getConsumerGroupID() {
        return consumerGroupID;
    }

    @Override
    public DataPartition.DataPartitionType getDataPartitionType() {
        return dataPartitionType;
    }

    @Override
    public DiskType getDiskType() {
        return diskType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultShuffleResource that = (DefaultShuffleResource) o;
        if (shuffleWorkerDescriptors.length != that.shuffleWorkerDescriptors.length) {
            return false;
        }

        if (!dataPartitionType.equals(that.dataPartitionType)) {
            return false;
        }

        if (!diskType.equals(that.diskType)) {
            return false;
        }

        if (consumerGroupID != that.consumerGroupID) {
            return false;
        }

        for (int i = 0; i < shuffleWorkerDescriptors.length; i++) {
            if (!Objects.equals(shuffleWorkerDescriptors[i], that.shuffleWorkerDescriptors[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result =
                StringUtils.isBlank(dataPartitionType.toString())
                        ? 0
                        : dataPartitionType.hashCode();
        result = result * 31 + Objects.hash(consumerGroupID);
        result = result * 31 + Objects.hash(diskType);
        for (ShuffleWorkerDescriptor shuffleWorkerDescriptor : shuffleWorkerDescriptors) {
            result = result * 31 + Objects.hash(shuffleWorkerDescriptor);
        }
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        for (int i = 0; i < shuffleWorkerDescriptors.length; i++) {
            sb.append(shuffleWorkerDescriptors[i].toString());
            if (i < shuffleWorkerDescriptors.length - 1) {
                sb.append(",");
            }
        }

        if (!StringUtils.isBlank(dataPartitionType.toString())) {
            sb.append(",").append("dataPartitionType=").append(dataPartitionType);
        }
        sb.append(",").append("diskType=").append(diskType);
        sb.append(",").append("consumerGroupID=").append(consumerGroupID);
        sb.append("}");

        return sb.toString();
    }
}
