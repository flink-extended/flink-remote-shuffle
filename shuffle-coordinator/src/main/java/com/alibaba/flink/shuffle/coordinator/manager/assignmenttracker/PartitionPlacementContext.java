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

import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * A context class which is used when choose the next worker to store a new data partition. This is
 * a runtime context, while the other variables are placed in the fields of each different strategy.
 */
public class PartitionPlacementContext {

    private final DataPartitionFactory partitionFactory;

    PartitionPlacementContext(DataPartitionFactory partitionFactory) {
        checkArgument(partitionFactory != null, "Must be not null.");
        this.partitionFactory = checkNotNull(partitionFactory);
    }

    DataPartitionFactory getPartitionFactory() {
        return partitionFactory;
    }
}
