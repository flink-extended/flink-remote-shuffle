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

package com.alibaba.flink.shuffle.metrics.reporter;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.utils.NetUtils;

import com.alibaba.metrics.common.config.MetricsCollectPeriodConfig;
import com.alibaba.metrics.reporter.MetricManagerReporter;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.INTERVAL;
import static com.alibaba.metrics.MetricManager.getIMetricManager;

/** {@link MetricReporterFactory} for {@link PrometheusReporter}. */
public class PrometheusReporterFactory implements MetricReporterFactory {
    private static final String ARG_PORT = "port";
    private static final String DEFAULT_PORT = "9249";

    @Override
    public MetricManagerReporter createMetricReporter(Properties conf) {
        Configuration configuration = new Configuration(conf);
        String portsConfig = configuration.getString(ARG_PORT, DEFAULT_PORT);
        Iterator<Integer> ports = NetUtils.getPortRangeFromString(portsConfig);
        Duration interval =
                configuration.getDuration(
                        INTERVAL, Duration.ofMinutes(MetricsCollectPeriodConfig.DEFAULT_INTERVAL));
        PrometheusReporter.Builder builder = new PrometheusReporter.Builder(getIMetricManager());
        builder.ports(ports);
        builder.metricsReportPeriodConfig(
                new MetricsCollectPeriodConfig((int) interval.getSeconds()));

        // start reporter
        MetricManagerReporter reporter = builder.build();
        reporter.start(interval.getSeconds(), TimeUnit.SECONDS);
        return reporter;
    }
}
