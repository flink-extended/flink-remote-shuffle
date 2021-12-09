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

import com.alibaba.metrics.IMetricManager;
import com.alibaba.metrics.Metric;
import com.alibaba.metrics.MetricFilter;
import com.alibaba.metrics.common.config.MetricsCollectPeriodConfig;
import com.alibaba.metrics.reporter.MetricManagerReporter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import org.glassfish.jersey.internal.guava.Preconditions;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link MetricManagerReporter} that exports {@link Metric Metrics} via Prometheus {@link
 * PushGateway}.
 */
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter {
    private final PushGateway pushGateway;
    private final String jobName;
    private final Map<String, String> groupingKey;
    private final boolean deleteOnShutdown;
    private static final String INIT_FLAG = "com.alibaba.metrics.file_reporter.init_flag";
    private static final String REPORTER_NAME = "prometheus-pushGateway-reporter";

    private PrometheusPushGatewayReporter(
            String host,
            int port,
            String jobName,
            Map<String, String> groupingKey,
            final boolean deleteOnShutdown,
            IMetricManager metricManager,
            MetricFilter filter,
            MetricsCollectPeriodConfig metricsReportPeriodConfig,
            TimeUnit rateUnit,
            TimeUnit durationUnit) {
        super(
                metricManager,
                REPORTER_NAME,
                filter,
                metricsReportPeriodConfig,
                rateUnit,
                durationUnit);
        this.pushGateway = new PushGateway(host + ':' + port);
        this.jobName = Preconditions.checkNotNull(jobName);
        this.groupingKey = Preconditions.checkNotNull(groupingKey);
        this.deleteOnShutdown = deleteOnShutdown;
    }

    @Override
    public void start(long period, TimeUnit unit) {
        String initFlag = System.getProperty(INIT_FLAG);

        if ("false".equals(initFlag)) {
            log.info("PrometheusPushGatewayReporter disabled...");
            return;
        }

        if (initFlag == null) {
            System.setProperty(INIT_FLAG, "true");
            super.start(period, unit);
        } else {
            log.info("PrometheusPushGatewayReporter has been started...");
        }
    }

    @Override
    public void report() {
        try {
            super.report();
            pushGateway.push(CollectorRegistry.defaultRegistry, jobName, groupingKey);
        } catch (Exception e) {
            log.warn(
                    "Failed to push metrics to PushGateway with jobName {}, groupingKey {}.",
                    jobName,
                    groupingKey,
                    e);
        }
    }

    @Override
    public void close() {
        if (deleteOnShutdown && pushGateway != null) {
            try {
                pushGateway.delete(jobName, groupingKey);
            } catch (IOException e) {
                log.warn(
                        "Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.",
                        jobName,
                        groupingKey,
                        e);
            }
        }
        super.close();
    }

    /** A builder for PrometheusPushGatewayReporter instances. */
    public static class Builder {
        private String host;
        private int port;
        private String jobName;
        private Map<String, String> groupingKey;
        private boolean deleteOnShutdown;
        private MetricFilter filter;
        private IMetricManager metricManager;
        private MetricsCollectPeriodConfig metricsReportPeriodConfig;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;

        // 提交到服务器的时间戳的单位，只支持毫秒和秒，默认是秒
        private TimeUnit timestampPrecision = TimeUnit.SECONDS;

        Builder(IMetricManager metricManager) {
            this.metricManager = metricManager;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public PrometheusPushGatewayReporter.Builder host(String host) {
            this.host = host;
            return this;
        }

        public PrometheusPushGatewayReporter.Builder port(int port) {
            this.port = port;
            return this;
        }

        public PrometheusPushGatewayReporter.Builder jobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public PrometheusPushGatewayReporter.Builder groupingKey(Map<String, String> groupingKey) {
            this.groupingKey = groupingKey;
            return this;
        }

        public PrometheusPushGatewayReporter.Builder deleteOnShutdown(boolean deleteOnShutdown) {
            this.deleteOnShutdown = deleteOnShutdown;
            return this;
        }

        /**
         * @param metricsReportPeriodConfig
         * @return
         */
        public PrometheusPushGatewayReporter.Builder metricsReportPeriodConfig(
                MetricsCollectPeriodConfig metricsReportPeriodConfig) {
            this.metricsReportPeriodConfig = metricsReportPeriodConfig;
            return this;
        }

        public PrometheusPushGatewayReporter build() {
            return new PrometheusPushGatewayReporter(
                    host,
                    port,
                    jobName,
                    groupingKey,
                    deleteOnShutdown,
                    metricManager,
                    filter,
                    metricsReportPeriodConfig,
                    rateUnit,
                    durationUnit);
        }
    }
}
