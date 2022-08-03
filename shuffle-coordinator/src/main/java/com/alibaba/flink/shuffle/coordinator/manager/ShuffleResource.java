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

import java.io.Serializable;

/**
 * The interface represents the resource that a task could shuffle-read from or shuffle-write to.
 */
public interface ShuffleResource extends Serializable {

    ShuffleWorkerDescriptor[] getReducePartitionLocations();

    ShuffleWorkerDescriptor getMapPartitionLocation();

    void setConsumerGroupID(long consumerGroupID);

    long getConsumerGroupID();

    DataPartition.DataPartitionType getDataPartitionType();

    DiskType getDiskType();
}
