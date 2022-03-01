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

package com.alibaba.flink.shuffle.coordinator.metrics;

import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;

import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Meter;

import java.util.function.Supplier;

/** Constants and util methods of CLUSTER metrics. */
public class ClusterMetricsUtil {

    /** Metric group name. */
    public static final String CLUSTER = "remote-shuffle.cluster";

    /** Number of available shuffle workers. */
    public static final String NUM_SHUFFLE_WORKERS = CLUSTER + ".num_shuffle_workers";

    /** Number of jobs under serving. */
    public static final String NUM_JOBS_SERVING = CLUSTER + ".num_jobs_serving";

    /** Total number of data partitions stored. */
    public static final String TOTAL_NUM_DATA_PARTITIONS = CLUSTER + ".total_num_data_partitions";

    /** Shuffle resource request throughput. */
    public static final String RESOURCE_REQUEST_THROUGHPUT =
            CLUSTER + ".resource_request_throughput";

    /** Maximum free space in bytes of all HDDs in all shuffle workers. */
    public static final String HDD_MAX_FREE_BYTES = CLUSTER + ".hdd_max_free_bytes";

    /** Maximum free space in bytes of all SSDs in all shuffle workers. */
    public static final String SSD_MAX_FREE_BYTES = CLUSTER + ".ssd_max_free_bytes";

    /** Maximum used space in bytes of all HDDs in all shuffle workers. */
    public static final String HDD_MAX_USED_BYTES = CLUSTER + ".hdd_max_used_bytes";

    /** Maximum used space in bytes of all SSDs in all shuffle workers. */
    public static final String SSD_MAX_USED_BYTES = CLUSTER + ".ssd_max_used_bytes";

    public static void registerNumShuffleWorkers(Supplier<Integer> numShuffleWorkers) {
        MetricUtils.registerMetric(
                CLUSTER,
                NUM_SHUFFLE_WORKERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return numShuffleWorkers.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerNumJobsServing(Supplier<Integer> numJobsServing) {
        MetricUtils.registerMetric(
                CLUSTER,
                NUM_JOBS_SERVING,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return numJobsServing.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerTotalNumDataPartitions(Supplier<Long> totalNumDataPartitions) {
        MetricUtils.registerMetric(
                CLUSTER,
                TOTAL_NUM_DATA_PARTITIONS,
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return totalNumDataPartitions.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static Meter registerResourceRequestThroughput() {
        return MetricUtils.getMeter(CLUSTER, RESOURCE_REQUEST_THROUGHPUT);
    }

    public static void registerHddMaxFreeBytes(Supplier<Long> hddMaxFreeBytes) {
        MetricUtils.registerMetric(
                CLUSTER,
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
                CLUSTER,
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
                CLUSTER,
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
                CLUSTER,
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
}
