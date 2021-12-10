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
import io.prometheus.client.exporter.HTTPServer;
import org.glassfish.jersey.internal.guava.Preconditions;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/** {@link MetricManagerReporter} that exports {@link Metric Metrics} via Prometheus. */
public class PrometheusReporter extends AbstractPrometheusReporter {
    private HTTPServer httpServer;
    private int port;
    private static final String INIT_FLAG = "com.alibaba.metrics.file_reporter.init_flag";
    private static final String REPORTER_NAME = "prometheus-reporter";

    int getPort() {
        Preconditions.checkState(httpServer != null, "Server has not been initialized.");
        return port;
    }

    protected PrometheusReporter(
            Iterator<Integer> ports,
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
        while (ports.hasNext()) {
            port = ports.next();
            try {
                // internally accesses CollectorRegistry.defaultRegistry
                httpServer = new HTTPServer(port);
                log.info("Started PrometheusReporter HTTP server on port {}.", port);
                break;
            } catch (IOException ioe) { // assume port conflict
                log.debug("Could not start PrometheusReporter HTTP server on port {}.", port, ioe);
            }
        }

        if (httpServer == null) {
            throw new RuntimeException(
                    "Could not start PrometheusReporter HTTP server on any configured port. Ports: "
                            + ports);
        }
    }

    @Override
    public void start(long period, TimeUnit unit) {
        String initFlag = System.getProperty(INIT_FLAG);

        if ("false".equals(initFlag)) {
            log.info("PrometheusReporter disabled...");
            return;
        }

        if (initFlag == null) {
            System.setProperty(INIT_FLAG, "true");
            super.start(period, unit);
        } else {
            log.info("PrometheusReporter has been started...");
        }
    }

    @Override
    public void close() {
        if (httpServer != null) {
            httpServer.stop();
        }
        super.close();
    }

    /** A builder for PrometheusReporter instances. */
    public static class Builder {
        private Iterator<Integer> ports;
        private MetricFilter filter;
        private IMetricManager metricManager;
        private MetricsCollectPeriodConfig metricsReportPeriodConfig;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;

        Builder(IMetricManager metricManager) {
            this.metricManager = metricManager;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        public PrometheusReporter.Builder ports(Iterator<Integer> ports) {
            this.ports = ports;
            return this;
        }

        /**
         * @param metricsReportPeriodConfig
         * @return
         */
        public PrometheusReporter.Builder metricsReportPeriodConfig(
                MetricsCollectPeriodConfig metricsReportPeriodConfig) {
            this.metricsReportPeriodConfig = metricsReportPeriodConfig;
            return this;
        }

        public PrometheusReporter build() {
            return new PrometheusReporter(
                    ports,
                    metricManager,
                    filter,
                    metricsReportPeriodConfig,
                    rateUnit,
                    durationUnit);
        }
    }
}
