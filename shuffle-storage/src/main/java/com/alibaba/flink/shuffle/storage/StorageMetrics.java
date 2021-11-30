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

package com.alibaba.flink.shuffle.storage;

import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;

import com.alibaba.metrics.Counter;
import com.alibaba.metrics.Gauge;

import java.util.function.Supplier;

/** Constants and util metrics of STORAGE metrics. */
public class StorageMetrics {

    // Group name
    public static final String STORAGE = "remote-shuffle.storage";

    // Available writing buffers in buffer pool.
    public static final String NUM_AVAILABLE_WRITING_BUFFERS =
            STORAGE + ".num_available_writing_buffers";

    // Available reading buffers in buffer pool.
    public static final String NUM_AVAILABLE_READING_BUFFERS =
            STORAGE + ".num_available_reading_buffers";

    // Number of data partitions stored.
    public static final String NUM_DATA_PARTITIONS = STORAGE + ".num_data_partitions";

    // Shuffle data size in bytes.
    public static final String NUM_BYTES_DATA = STORAGE + ".data_size_bytes";

    // Index data size in bytes.
    public static final String NUM_BYTES_INDEX = STORAGE + ".index_size_bytes";

    public static void registerGaugeForNumAvailableWritingBuffers(Supplier<Integer> availableNum) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_AVAILABLE_WRITING_BUFFERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return availableNum.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerGaugeForNumAvailableReadingBuffers(Supplier<Integer> availableNum) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_AVAILABLE_READING_BUFFERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return availableNum.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static Counter registerCounterForNumDataPartitions() {
        return MetricUtils.getCounter(STORAGE, NUM_DATA_PARTITIONS);
    }

    public static void registerGaugeForNumBytesDataSize(Supplier<Long> dataSizeBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_BYTES_DATA,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return dataSizeBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerGaugeForNumBytesIndexSize(Supplier<Long> indexSizeBytes) {
        MetricUtils.registerMetric(
                STORAGE,
                NUM_BYTES_INDEX,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return indexSizeBytes.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }
}
