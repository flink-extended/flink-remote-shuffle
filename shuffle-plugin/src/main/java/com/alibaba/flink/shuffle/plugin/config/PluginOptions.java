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

package com.alibaba.flink.shuffle.plugin.config;

import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;

/** Config options for shuffle jobs using the remote shuffle service. */
public class PluginOptions {

    public static final MemorySize MIN_MEMORY_PER_PARTITION = MemorySize.parse("8m");

    public static final MemorySize MIN_MEMORY_PER_GATE = MemorySize.parse("8m");

    /**
     * Identify the remote shuffle cluster to use for the job. It is treated as a name prefix, which
     * means a job can only use the shuffle cluster whose cluster ID starts with the configured
     * value. For example, if a shuffle cluster is configured with cluster ID 'ab', then the job
     * configured with ID prefix 'a' or 'ab' can use it, but the job configured with ID prefix 'b'
     * or 'ac' can't use it.
     */
    public static final ConfigOption<String> TARGET_CLUSTER_ID_PREFIX =
            new ConfigOption<String>("remote-shuffle.cluster.id-prefix")
                    .defaultValue(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.defaultValue())
                    .description(
                            "Identify the remote shuffle cluster to use for the job. It is treated "
                                    + "as a name prefix, which means a job can only use the shuffle"
                                    + " cluster whose cluster ID starts with the configured value. "
                                    + "For example, if a shuffle cluster is configured with cluster"
                                    + " ID 'ab', then the job configured with ID prefix 'a' or 'ab'"
                                    + " can use it, but the job configured with ID prefix 'b' or "
                                    + "'ac' can't use it.");

    /**
     * The maximum number of remote shuffle channels to open and read concurrently per input gate.
     */
    public static final ConfigOption<Integer> NUM_CONCURRENT_READINGS =
            new ConfigOption<Integer>("remote-shuffle.job.concurrent-readings-per-gate")
                    .defaultValue(Integer.MAX_VALUE)
                    .description(
                            "The maximum number of remote shuffle channels to open and read "
                                    + "concurrently per input gate.");

    /**
     * The size of network buffers required per result partition. The minimum valid value is 8M.
     * Usually, several hundreds of megabytes memory is enough for large scale batch jobs.
     */
    public static final ConfigOption<MemorySize> MEMORY_PER_RESULT_PARTITION =
            new ConfigOption<MemorySize>("remote-shuffle.job.memory-per-partition")
                    .defaultValue(MemorySize.parse("64m"))
                    .description(
                            "The size of network buffers required per result partition. The "
                                    + "minimum valid value is 8M. Usually, several hundreds of "
                                    + "megabytes memory is enough for large scale batch jobs.");

    /**
     * The size of network buffers required per input gate. The minimum valid value is 8m. Usually,
     * several hundreds of megabytes memory is enough for large scale batch jobs.
     */
    public static final ConfigOption<MemorySize> MEMORY_PER_INPUT_GATE =
            new ConfigOption<MemorySize>("remote-shuffle.job.memory-per-gate")
                    .defaultValue(MemorySize.parse("32m"))
                    .description(
                            "The size of network buffers required per input gate. The minimum "
                                    + "valid value is 8m. Usually, several hundreds of megabytes "
                                    + "memory is enough for large scale batch jobs.");

    /**
     * Defines the factory used to create new data partitions. According to the specified data
     * partition factory from the client side, the shuffle manager will return corresponding
     * resources and the shuffle worker will create the corresponding partitions.
     */
    public static final ConfigOption<String> DATA_PARTITION_FACTORY_NAME =
            new ConfigOption<String>("remote-shuffle.job.data-partition-factory-name")
                    .defaultValue(
                            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory")
                    .description(
                            "Defines the factory used to create new data partitions. According to "
                                    + "the specified data partition factory from the client side, "
                                    + "the shuffle manager will return corresponding resources and"
                                    + " the shuffle worker will create the corresponding partitions.");

    /**
     * Whether to enable shuffle data compression. Usually, enabling data compression can save the
     * storage space and achieve better performance.
     */
    public static final ConfigOption<Boolean> ENABLE_DATA_COMPRESSION =
            new ConfigOption<Boolean>("remote-shuffle.job.enable-data-compression")
                    .defaultValue(true)
                    .description(
                            "Whether to enable shuffle data compression. Usually, enabling data "
                                    + "compression can save the storage space and achieve better "
                                    + "performance.");

    /**
     * Whether to shuffle the reading channels for better load balance of the downstream consumer
     * tasks.
     */
    public static final ConfigOption<Boolean> SHUFFLE_READING_CHANNELS =
            new ConfigOption<Boolean>("remote-shuffle.job.shuffle-reading-channels")
                    .defaultValue(true)
                    .description(
                            "Whether to shuffle the reading channels for better load balance of the"
                                    + " downstream consumer tasks.");
}
