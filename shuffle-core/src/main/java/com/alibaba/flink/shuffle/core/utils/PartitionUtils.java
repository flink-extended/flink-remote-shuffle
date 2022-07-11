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

    public static final String HDD_MAP_PARTITION_FACTORY =
            "com.alibaba.flink.shuffle.storage.partition.HDDOnlyLocalFileMapPartitionFactory";

    public static final String SSD_MAP_PARTITION_FACTORY =
            "com.alibaba.flink.shuffle.storage.partition.SSDOnlyLocalFileMapPartitionFactory";

    public static final String MAP_PARTITION_FACTORY =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory";

    public static final String HDD_REDUCE_PARTITION_FACTORY =
            "com.alibaba.flink.shuffle.storage.partition.HDDOnlyLocalFileReducePartitionFactory";

    public static final String SSD_REDUCE_PARTITION_FACTORY =
            "com.alibaba.flink.shuffle.storage.partition.SSDOnlyLocalFileReducePartitionFactory";

    public static final String REDUCE_PARTITION_FACTORY =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileReducePartitionFactory";

    public static String convertToMapPartitionFactory(String partitionFactory) {
        switch (partitionFactory) {
            case HDD_REDUCE_PARTITION_FACTORY:
                return HDD_MAP_PARTITION_FACTORY;
            case SSD_REDUCE_PARTITION_FACTORY:
                return SSD_MAP_PARTITION_FACTORY;
            case REDUCE_PARTITION_FACTORY:
                return MAP_PARTITION_FACTORY;
            default:
                return partitionFactory;
        }
    }

    public static boolean isMapPartitionFactory(String partitionFactory) {
        return partitionFactory.equals(HDD_MAP_PARTITION_FACTORY)
                || partitionFactory.equals(SSD_MAP_PARTITION_FACTORY)
                || partitionFactory.equals(MAP_PARTITION_FACTORY);
    }

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
