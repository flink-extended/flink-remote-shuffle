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
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.StringUtils;

import com.alibaba.metrics.common.config.MetricsCollectPeriodConfig;
import com.alibaba.metrics.reporter.MetricManagerReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.HOST;
import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.INTERVAL;
import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.PORT;
import static com.alibaba.flink.shuffle.metrics.reporter.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;
import static com.alibaba.metrics.MetricManager.getIMetricManager;

/** {@link MetricReporterFactory} for {@link PrometheusPushGatewayReporter}. */
public class PrometheusPushGatewayReporterFactory implements MetricReporterFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(PrometheusPushGatewayReporterFactory.class);

    @Override
    public MetricManagerReporter createMetricReporter(Properties conf) {
        Configuration configuration = new Configuration(conf);
        String host = configuration.getString(HOST.key(), HOST.defaultValue());
        int port = configuration.getInteger(PORT.key(), PORT.defaultValue());
        String configuredJobName = configuration.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
        boolean randomSuffix =
                configuration.getBoolean(
                        RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());
        boolean deleteOnShutdown =
                configuration.getBoolean(
                        DELETE_ON_SHUTDOWN.key(), DELETE_ON_SHUTDOWN.defaultValue());
        Map<String, String> groupingKey =
                parseGroupingKey(
                        configuration.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));

        Duration interval =
                configuration.getDuration(
                        INTERVAL, Duration.ofMinutes(MetricsCollectPeriodConfig.DEFAULT_INTERVAL));
        if (host == null || host.isEmpty() || port < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        }

        String jobName = configuredJobName;
        if (randomSuffix) {
            jobName = configuredJobName + CommonUtils.randomHexString(16);
        }

        PrometheusPushGatewayReporter.Builder builder =
                new PrometheusPushGatewayReporter.Builder(getIMetricManager());
        builder.port(port);
        builder.host(host);
        builder.jobName(jobName);
        builder.deleteOnShutdown(deleteOnShutdown);
        builder.groupingKey(groupingKey);
        builder.metricsReportPeriodConfig(
                new MetricsCollectPeriodConfig((int) interval.getSeconds()));

        // start reporter
        MetricManagerReporter reporter = builder.build();
        reporter.start(interval.getSeconds(), TimeUnit.SECONDS);
        return reporter;
    }

    static Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    LOG.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                    continue;
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)
                        || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                    LOG.warn(
                            "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
                            labelKey,
                            labelValue);
                    continue;
                }
                groupingKey.put(labelKey, labelValue);
            }

            return groupingKey;
        }

        return Collections.emptyMap();
    }
}
