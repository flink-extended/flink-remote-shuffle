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

package com.alibaba.flink.shuffle.metrics.reporter.prometheus;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.utils.ConfigurationParserUtils;
import com.alibaba.flink.shuffle.metrics.reporter.MetricReporterFactory;

import com.alibaba.metrics.common.config.MetricsCollectPeriodConfig;
import com.alibaba.metrics.reporter.MetricManagerReporter;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.metrics.reporter.prometheus.PrometheusReporterOptions.INTERVAL;
import static com.alibaba.metrics.MetricManager.getIMetricManager;

/** {@link MetricReporterFactory} for {@link PrometheusReporter}. */
public class PrometheusReporterFactory implements MetricReporterFactory {

    @Override
    public MetricManagerReporter createMetricReporter(Properties conf) {
        Configuration configuration = new Configuration(conf);
        String portsConfig = configuration.getString(PrometheusReporterOptions.PORTS);
        Iterator<Integer> ports = ConfigurationParserUtils.getPortRangeFromString(portsConfig);
        Duration interval = configuration.getDuration(INTERVAL);
        Map<String, String> commonTags =
                ConfigurationParserUtils.parseIntoMap(
                        configuration.getString(PrometheusReporterOptions.COMMON_TAGS));

        PrometheusReporter.Builder builder = new PrometheusReporter.Builder(getIMetricManager());
        builder.ports(ports);
        builder.commonTags(commonTags);
        builder.metricsReportPeriodConfig(
                new MetricsCollectPeriodConfig((int) interval.getSeconds()));

        // start reporter
        MetricManagerReporter reporter = builder.build();
        reporter.start(interval.getSeconds(), TimeUnit.SECONDS);
        return reporter;
    }
}
