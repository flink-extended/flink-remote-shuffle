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

package com.alibaba.flink.shuffle.core.config;

import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.core.storage.StorageType;

/** Config options for storage. */
public class StorageOptions {

    /** Minimum size of memory to be used for data partition writing and reading. */
    public static final MemorySize MIN_WRITING_READING_MEMORY_SIZE = MemorySize.parse("16m");

    /**
     * Maximum number of tolerable failures before marking a data partition as corrupted, which will
     * trigger the reproduction of the corresponding data.
     */
    public static final ConfigOption<Integer> STORAGE_FILE_TOLERABLE_FAILURES =
            new ConfigOption<Integer>("remote-shuffle.storage.file-tolerable-failures")
                    .defaultValue(Integer.MAX_VALUE)
                    .description(
                            "Maximum number of tolerable failures before marking a data partition "
                                    + "as corrupted, which will trigger the reproduction of the "
                                    + "corresponding data.");

    /**
     * Local file system directories to persist partitioned data to. Multiple directories can be
     * configured and these directories should be separated by comma (,). Each configured directory
     * can be attached with an optional label which indicates the disk type. The valid disk types
     * include 'SSD' and 'HDD'. If no label is offered, the default type would be 'HDD'. Here is a
     * simple valid configuration example: <b>[SSD]/dir1/,[HDD]/dir2/,/dir3/</b>. This option must
     * be configured and the configured dirs must exist.
     */
    public static final ConfigOption<String> STORAGE_LOCAL_DATA_DIRS =
            new ConfigOption<String>("remote-shuffle.storage.local-data-dirs")
                    .defaultValue(null)
                    .description(
                            "Local file system directories to persist partitioned data to. Multiple"
                                    + " directories can be configured and these directories should "
                                    + "be separated by comma (,). Each configured directory can be "
                                    + "attached with an optional label which indicates the disk "
                                    + "type. The valid disk types include 'SSD' and 'HDD'. If no "
                                    + "label is offered, the default type would be 'HDD'. Here is a"
                                    + " simple valid configuration example: '[SSD]/dir1/,[HDD]/dir2"
                                    + "/,/dir3/'. This option must be configured and the configured"
                                    + " directories must exist.");

    /**
     * Preferred disk type to use for data storage. The valid types include 'SSD' and 'HDD'. If
     * there are disks of the preferred type, only those disks will be used. However, this is not a
     * strict restriction, which means if there is no disk of the preferred type, disks of other
     * types will be also used.
     */
    public static final ConfigOption<String> STORAGE_PREFERRED_TYPE =
            new ConfigOption<String>("remote-shuffle.storage.preferred-disk-type")
                    .defaultValue(StorageType.SSD.name())
                    .description(
                            "Preferred disk type to use for data storage. The valid types include "
                                    + "'SSD' and 'HDD'. If there are disks of the preferred type, "
                                    + "only those disks will be used. However, this is not a strict "
                                    + "restriction, which means if there is no disk of the preferred"
                                    + " type, disks of other types will be also used.");

    /**
     * Number of threads to be used by data store for data partition processing of each HDD. The
     * actual number of threads per disk will be min[configured value, 4 * (number of processors)].
     */
    public static final ConfigOption<Integer> STORAGE_NUM_THREADS_PER_HDD =
            new ConfigOption<Integer>("remote-shuffle.storage.hdd.num-executor-threads")
                    .defaultValue(8)
                    .description(
                            "Number of threads to be used by data store for data partition processing"
                                    + " of each HDD. The actual number of threads per disk will be "
                                    + "min[configured value, 4 * (number of processors)].");

    /**
     * Number of threads to be used by data store for data partition processing of each SSD. The
     * actual number of threads per disk will be min[configured value, 4 * (number of processors)].
     */
    public static final ConfigOption<Integer> STORAGE_SSD_NUM_EXECUTOR_THREADS =
            new ConfigOption<Integer>("remote-shuffle.storage.ssd.num-executor-threads")
                    .defaultValue(Integer.MAX_VALUE)
                    .description(
                            "Number of threads to be used by data store for data partition processing"
                                    + " of each SSD. The actual number of threads per disk will be "
                                    + "min[configured value, 4 * (number of processors)].");

    /**
     * Number of threads to be used by data store for in-memory data partition processing. The
     * actual number of threads used will be min[configured value, 4 * (number of processors)].
     */
    public static final ConfigOption<Integer> STORAGE_MEMORY_NUM_EXECUTOR_THREADS =
            new ConfigOption<Integer>("remote-shuffle.storage.memory.num-executor-threads")
                    .defaultValue(Integer.MAX_VALUE)
                    .description(
                            "Number of threads to be used by data store for in-memory data partition"
                                    + " processing. The actual number of threads used will be min["
                                    + "configured value, 4 * (number of processors)].");

    /**
     * Maximum memory size to use for the data writing of each data partition. Note that if the
     * configured value is smaller than {@link #MIN_WRITING_READING_MEMORY_SIZE}, the minimum {@link
     * #MIN_WRITING_READING_MEMORY_SIZE} will be used.
     */
    public static final ConfigOption<MemorySize> STORAGE_MAX_PARTITION_WRITING_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.storage.partition.max-writing-memory")
                    .defaultValue(MemorySize.parse("128m"))
                    .description(
                            String.format(
                                    "Maximum memory size to use for the data writing of each data "
                                            + "partition. Note that if the configured value is "
                                            + "smaller than %s, the minimum %s will be used.",
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString(),
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString()));

    /**
     * Maximum memory size to use for the data reading of each data partition. Note that if the
     * configured value is smaller than {@link #MIN_WRITING_READING_MEMORY_SIZE}, the minimum {@link
     * #MIN_WRITING_READING_MEMORY_SIZE} will be used.
     */
    public static final ConfigOption<MemorySize> STORAGE_MAX_PARTITION_READING_MEMORY =
            new ConfigOption<MemorySize>("remote-shuffle.storage.partition.max-reading-memory")
                    .defaultValue(MemorySize.parse("128m"))
                    .description(
                            String.format(
                                    "Maximum memory size to use for the data reading of each data "
                                            + "partition. Note that if the configured value is "
                                            + "smaller than %s, the minimum %s will be used.",
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString(),
                                    MIN_WRITING_READING_MEMORY_SIZE.toHumanReadableString()));

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private StorageOptions() {}
}
