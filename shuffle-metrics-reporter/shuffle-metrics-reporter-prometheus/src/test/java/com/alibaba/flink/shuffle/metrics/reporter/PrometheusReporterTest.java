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
import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;

import com.alibaba.metrics.ClusterHistogram;
import com.alibaba.metrics.Compass;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.IMetricManager;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.Metric;
import com.alibaba.metrics.MetricFilter;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.Timer;
import com.alibaba.metrics.reporter.MetricManagerReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;
import java.util.Properties;

import static com.alibaba.metrics.MetricManager.getIMetricManager;

/** Basic test for {@link PrometheusReporter}. */
public class PrometheusReporterTest extends TestLogger {
    private static final PrometheusReporterFactory prometheusReporterFactory =
            new PrometheusReporterFactory();

    @Rule public ExpectedException thrown = ExpectedException.none();
    private IMetricManager metricManager;

    private MetricManagerReporter reporter;

    @Before
    public void setupReporter() {
        Properties properties = new Properties();
        Configuration conf = new Configuration(properties);
        metricManager = getIMetricManager();
        reporter = prometheusReporterFactory.createMetricReporter(conf.toProperties());
    }

    @After
    public void shutdownRegistry() throws Exception {
        if (reporter != null) {
            reporter.close();
        }
    }

    /**
     * {@link io.prometheus.client.Counter} may not decrease, so report {@link Counter} as {@link
     * io.prometheus.client.Gauge}.
     *
     * @throws Exception Might be thrown on HTTP problems.
     */
    @Test
    public void counterIsReportedAsPrometheusGauge() throws Exception {
        Counter testCounter = MetricUtils.getCounter("test", "testCounter");
        testCounter.inc(7);
        Map<Class<? extends Metric>, Map<MetricName, ? extends Metric>> categoryMetrics =
                metricManager.getAllCategoryMetrics(MetricFilter.ALL);
        reporter.report(
                (Map<MetricName, Gauge>) categoryMetrics.get(Gauge.class),
                (Map<MetricName, Counter>) categoryMetrics.get(Counter.class),
                (Map<MetricName, Histogram>) categoryMetrics.get(Histogram.class),
                (Map<MetricName, Meter>) categoryMetrics.get(Meter.class),
                (Map<MetricName, Timer>) categoryMetrics.get(Timer.class),
                (Map<MetricName, Compass>) categoryMetrics.get(Compass.class),
                (Map<MetricName, FastCompass>) categoryMetrics.get(FastCompass.class),
                (Map<MetricName, ClusterHistogram>) categoryMetrics.get(ClusterHistogram.class));
    }
}
