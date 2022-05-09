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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** A utility class to load partition placement strategy factories from the configuration. */
public final class PartitionPlacementStrategyLoader {

    private static final Logger LOG =
            LoggerFactory.getLogger(PartitionPlacementStrategyLoader.class);

    /** Config name for the {@link MinNumberPlacementStrategy}. */
    public static final String MIN_NUM_PLACEMENT_STRATEGY_NAME = "min-num";

    /** Config name for the {@link RandomPlacementStrategy}. */
    public static final String RANDOM_PLACEMENT_STRATEGY_NAME = "random";

    /** Config name for the {@link RoundRobinPlacementStrategy}. */
    public static final String ROUND_ROBIN_PLACEMENT_STRATEGY_NAME = "round-robin";

    /** Config name for the {@link LocalityPlacementStrategy}. */
    public static final String LOCALITY_PLACEMENT_STRATEGY_NAME = "locality";

    private PartitionPlacementStrategyLoader() {}

    /**
     * Loads a {@link PartitionPlacementStrategy} from the given configuration.
     *
     * @param configuration which specifies the placement strategy factory to load
     * @return data partition placement strategy factory loaded
     */
    public static PartitionPlacementStrategy loadPlacementStrategyFactory(
            final Configuration configuration) {
        checkNotNull(configuration);
        long minReservedSpaceBytes =
                configuration
                        .getMemorySize(StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES)
                        .getBytes();
        long maxUsableSpaceBytes =
                configuration
                        .getMemorySize(StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES)
                        .getBytes();
        String strategyParam = configuration.getString(ManagerOptions.PARTITION_PLACEMENT_STRATEGY);
        LOG.info("Using the data partition placement strategy: {}.", strategyParam);

        switch (strategyParam.toLowerCase()) {
            case MIN_NUM_PLACEMENT_STRATEGY_NAME:
                return new MinNumberPlacementStrategy(minReservedSpaceBytes, maxUsableSpaceBytes);
            case RANDOM_PLACEMENT_STRATEGY_NAME:
                return new RandomPlacementStrategy(minReservedSpaceBytes, maxUsableSpaceBytes);
            case ROUND_ROBIN_PLACEMENT_STRATEGY_NAME:
                return new RoundRobinPlacementStrategy(minReservedSpaceBytes, maxUsableSpaceBytes);
            case LOCALITY_PLACEMENT_STRATEGY_NAME:
                return new LocalityPlacementStrategy(minReservedSpaceBytes, maxUsableSpaceBytes);
            default:
                throw new IllegalArgumentException(
                        "Unknown partition placement strategy: " + strategyParam);
        }
    }
}
