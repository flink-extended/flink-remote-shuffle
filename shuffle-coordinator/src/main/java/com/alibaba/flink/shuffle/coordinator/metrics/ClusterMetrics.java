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

import java.util.function.Supplier;

/** Constants and util methods of CLUSTER metrics. */
public class ClusterMetrics {

    // Group name
    public static final String CLUSTER = "remote-shuffle.cluster";

    // Number of available shuffle workers.
    public static final String NUM_SHUFFLE_WORKERS = CLUSTER + ".num_shuffle_workers";

    // Number of jobs under serving.
    public static final String NUM_JOBS_SERVING = CLUSTER + ".num_jobs_serving";

    public static void registerGaugeForNumShuffleWorkers(Supplier<Integer> shuffleWorkerNum) {
        MetricUtils.registerMetric(
                CLUSTER,
                NUM_SHUFFLE_WORKERS,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return shuffleWorkerNum.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }

    public static void registerGaugeForNumJobsServing(Supplier<Integer> jobsServingNum) {
        MetricUtils.registerMetric(
                CLUSTER,
                NUM_JOBS_SERVING,
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return jobsServingNum.get();
                    }

                    @Override
                    public long lastUpdateTime() {
                        return System.currentTimeMillis();
                    }
                });
    }
}
