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

package com.alibaba.flink.shuffle.core.utils;

import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;

/** Utility methods to manipulate data partitions. */
public class PartitionUtils {

    public static boolean isMapPartition(String dataPartitionFactoryName) {
        return isMapPartition(getDataPartitionType(dataPartitionFactoryName));
    }

    public static boolean isMapPartition(DataPartition.DataPartitionType partitionType) {
        return partitionType.equals(DataPartition.DataPartitionType.MAP_PARTITION);
    }

    public static DataPartition.DataPartitionType getDataPartitionType(
            String dataPartitionFactoryName) {
        try {
            return DataPartitionFactory.getDataPartitionType(dataPartitionFactoryName);
        } catch (Throwable throwable) {
            throw new RuntimeException("Failed to get data partition type, ", throwable);
        }
    }
}
