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

package com.alibaba.flink.shuffle.storage;

import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;

import com.alibaba.metrics.Counter;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Meter;

import java.util.function.Supplier;

/** Constants and util metrics of STORAGE metrics. */
public class StorageMetricsUtil {

    /** Metric group name. */
    public static final String STORAGE = "remote-shuffle.storage";

    /** Number of total executors for data partition processing. */
    public static final String TOTAL_NUM_EXECUTORS = STORAGE + ".total_num_executors";

    /** Total writing buffers in buffer pool. */
    public static final String TOTAL_NUM_WRITING_BUFFERS = STORAGE + ".total_num_writing_buffers";

    /** Total reading buffers in buffer pool. */
    public static final String TOTAL_NUM_READING_BUFFERS = STORAGE + ".total_num_reading_buffers";

    /** Available writing buffers in buffer pool. */
    public static final String NUM_AVAILABLE_WRITING_BUFFERS =
            STORAGE + ".num_available_writing_buffers";

    /** Available reading buffers in buffer pool. */
    public static final String NUM_AVAILABLE_READING_BUFFERS =
            STORAGE + ".num_available_reading_buffers";

    /** Time in milliseconds used fulfilling the last reading buffers request. */
    public static final String TIME_WAITING_READING_BUFFERS =
            STORAGE + ".time_waiting_reading_buffers";

    /** Time in milliseconds used fulfilling the last writing buffers request. */
    public static final String TIME_WAITING_WRITING_BUFFERS =
            STORAGE + ".time_waiting_writing_buffers";

    /** Maximum free space in bytes of all HDDs. */
    public static final String HDD_MAX_FREE_BYTES = STORAGE + ".hdd_max_free_bytes";

    /** Maximum free space in bytes of all SSDs. */
    public static final String SSD_MAX_FREE_BYTES = STORAGE + ".ssd_max_free_bytes";

    /** Maximum used space in bytes of all HDDs. */
    public static final String HDD_MAX_USED_BYTES = STORAGE + ".hdd_max_used_bytes";

    /** Maximum used space in bytes of all SSDs. */
    public static final String SSD_MAX_USED_BYTES = STORAGE + ".ssd_max_used_bytes";

    /** Total shuffle data and index file size in bytes. */
    public static final String TOTAL_PARTITION_FILE_BYTES = STORAGE + ".total_partition_file_bytes";

    /** Number of data partitions stored. */
    public static final String NUM_DATA_PARTITIONS = STORAGE + ".num_data_partitions";

    /** Total data file size in bytes. */
    public static final String TOTAL_DATA_FILE_BYTES = STORAGE + ".total_data_file_bytes";

    /** Total index file size in bytes. */
    public static final String TOTAL_INDEX_FILE_BYTES = STORAGE + ".total_index_file_bytes";

    /** Maximum data file size in bytes. */
    public static final String MAX_DATA_FILE_BYTES = STORAGE + ".max_data_file_bytes";

    /** Maximum index file size in bytes. */
    public static final String MAX_INDEX_FILE_BYTES = STORAGE + ".max_index_file_bytes";

    /** Average data file size in bytes. */
    public static final String AVG_DATA_FILE_BYTES = STORAGE + ".avg_data_file_bytes";

    /** Average index file size in bytes. */
    public static final String AVG_INDEX_FILE_BYTES = STORAGE + ".avg_index_file_bytes";

    /** Maximum number of data regions per data partition. */
    public static final String MAX_NUM_DATA_REGIONS = STORAGE + ".max_num_data_regions";

    /** Current writing throughput in bytes. */
    public static final String WRITING_THROUGHPUT_BYTES = STORAGE + ".writing_throughput_bytes";

    /** Current reading throughput in bytes. */
    public static final String READING_THROUGHPUT_BYTES = STORAGE + ".reading_throughput_bytes";

    public static final String NUM_WRITE_FAILS = STORAGE + ".num_write_fails";

    public static final String NUM_READ_FAILS = STORAGE + ".num_read_fails";

    public static void registerTotalNumExecutors(Supplier<Integer> totalNumExecutors) {
        MetricUtils.registerMetric(
                STORAGE,
                TOTAL_NUM_EXECUTORS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return totalNumExecutors.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTimeWaitingReadingBuffers(Supplier<Long> timeWaitingReadingBuffers) {
        MetricUtils.registerMetric(
                STORAGE,
                TIME_WAITING_READING_BUFFERS,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return timeWaitingReadingBuffers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTimeWaitingWritingBuffers(Supplier<Long> timeWaitingWritingBuffers) {
        MetricUtils.registerMetric(
                STORAGE,
                TIME_WAITING_WRITING_BUFFERS,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return timeWaitingWritingBuffers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTotalNumReadingBuffers(Supplier<Integer> totalNumReadingBuffers) {
        MetricUtils.registerMetric(
                STORAGE,
                TOTAL_NUM_READING_BUFFERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return totalNumReadingBuffers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTotalNumWritingBuffers(Supplier<Integer> totalNumWritingBuffers) {
        MetricUtils.registerMetric(
                STORAGE,
                TOTAL_NUM_WRITING_BUFFERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return totalNumWritingBuffers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerNumAvailableWritingBuffers(
            Supplier<Integer> numAvailableWritingBuffers) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_AVAILABLE_WRITING_BUFFERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return numAvailableWritingBuffers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerNumAvailableReadingBuffers(
            Supplier<Integer> numAvailableReadingBuffers) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_AVAILABLE_READING_BUFFERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return numAvailableReadingBuffers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerHddMaxFreeBytes(Supplier<Long> hddMaxFreeBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                HDD_MAX_FREE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return hddMaxFreeBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerSsdMaxFreeBytes(Supplier<Long> ssdMaxFreeBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                SSD_MAX_FREE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return ssdMaxFreeBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerHddMaxUsedBytes(Supplier<Long> hddMaxUsedBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                HDD_MAX_USED_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return hddMaxUsedBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerSsdMaxUsedBytes(Supplier<Long> ssdMaxUsedBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                SSD_MAX_USED_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return ssdMaxUsedBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTotalPartitionFileBytes(Supplier<Long> totalPartitionFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                TOTAL_PARTITION_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return totalPartitionFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerNumDataPartitions(Supplier<Integer> numDataPartitions) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_DATA_PARTITIONS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return numDataPartitions.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTotalDataFileBytes(Supplier<Long> totalDataFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                TOTAL_DATA_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return totalDataFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTotalIndexFileBytes(Supplier<Long> totalIndexFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                TOTAL_INDEX_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return totalIndexFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerMaxDataFileBytes(Supplier<Long> maxDataFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                MAX_DATA_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return maxDataFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerMaxIndexFileBytes(Supplier<Long> maxIndexFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                MAX_INDEX_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return maxIndexFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerAgvDataFileBytes(Supplier<Long> avgDataFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                AVG_DATA_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return avgDataFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerAvgIndexFileBytes(Supplier<Long> avgIndexFileBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                AVG_INDEX_FILE_BYTES,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return avgIndexFileBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerMaxNumDataRegions(Supplier<Long> maxNumDataRegions) {
        MetricUtils.registerMetric(
                STORAGE,
                MAX_NUM_DATA_REGIONS,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return maxNumDataRegions.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static Meter registerWritingThroughputBytes() {
        return MetricUtils.getMeter(STORAGE, WRITING_THROUGHPUT_BYTES);
    }

    public static Meter registerReadingThroughputBytes() {
        return MetricUtils.getMeter(STORAGE, READING_THROUGHPUT_BYTES);
    }

    public static Counter registerNumWriteFails() {
        return MetricUtils.getCounter(STORAGE, NUM_WRITE_FAILS);
    }

    public static Counter registerNumReadFails() {
        return MetricUtils.getCounter(STORAGE, NUM_READ_FAILS);
    }
}
