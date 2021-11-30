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

package com.alibaba.flink.shuffle.metrics.entry;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.metrics.reporter.ReporterSetup;

import com.alibaba.metrics.Compass;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.Metric;
import com.alibaba.metrics.MetricLevel;
import com.alibaba.metrics.MetricManager;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Utils to manager metrics. */
public class MetricUtils {
    private static final Logger LOG = LoggerFactory.getLogger(MetricUtils.class);

    // ---------------------------------------------------------------
    // Manage metric system
    // ---------------------------------------------------------------

    public static void startManagerMetricSystem(Configuration config) {
        MetricConfiguration metricConf = new MetricConfiguration(config, true);
        startMetricSystemInternal(metricConf);
    }

    public static void startWorkerMetricSystem(Configuration config) {
        MetricConfiguration metricConf = new MetricConfiguration(config, false);
        startMetricSystemInternal(metricConf);
    }

    private static void startMetricSystemInternal(MetricConfiguration conf) {
        try {
            MetricBootstrap.init(conf);
            ReporterSetup.fromConfiguration(conf.getConfiguration());
            LOG.info("Metric system start successfully");
        } catch (Throwable t) {
            LOG.error("Start metric system failed, ", t);
        }
    }

    public static void stopMetricSystem() {
        try {
            MetricBootstrap.destroy();
            LOG.info("Metric system is stopped.");
        } catch (Throwable throwable) {
            LOG.error("Failed to stop metric system.", throwable);
        }
    }

    // ---------------------------------------------------------------
    // Metrics Getter
    // ---------------------------------------------------------------

    /** Get {@link Counter}. */
    public static Counter getCounter(String groupName, String metricName, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getCounter(groupName, generateMetricName(metricName, tags));
    }

    /** Get {@link Counter}. */
    public static Counter getCounter(
            String groupName, String metricName, MetricLevel metricLevel, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getCounter(
                groupName, generateMetricName(metricName, metricLevel, tags));
    }

    /** Get {@link Meter}. */
    public static Meter getMeter(String groupName, String metricName, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getMeter(groupName, generateMetricName(metricName, tags));
    }

    /** Get {@link Meter}. */
    public static Meter getMeter(
            String groupName, String metricName, MetricLevel metricLevel, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getMeter(groupName, generateMetricName(metricName, metricLevel, tags));
    }

    /** Get {@link Histogram}. */
    public static Histogram getHistogram(String groupName, String metricName, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getHistogram(groupName, generateMetricName(metricName, tags));
    }

    /** Get {@link Histogram}. */
    public static Histogram getHistogram(
            String groupName, String metricName, MetricLevel metricLevel, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getHistogram(
                groupName, generateMetricName(metricName, metricLevel, tags));
    }

    /** Get {@link Timer}. */
    public static Timer getTimer(String groupName, String metricName, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getTimer(groupName, generateMetricName(metricName, tags));
    }

    /** Get {@link Timer}. */
    public static Timer getTimer(
            String groupName, String metricName, MetricLevel metricLevel, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getTimer(groupName, generateMetricName(metricName, metricLevel, tags));
    }

    /**
     * Get {@link Compass}. This metric is used when recording throughput, response time
     * distribution, success rate or error code metrics.
     */
    public static Compass getCompass(String groupName, String metricName, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getCompass(groupName, generateMetricName(metricName, tags));
    }

    /**
     * Get {@link Compass}. This metric is used when recording throughput, response time
     * distribution, success rate or error code metrics.
     */
    public static Compass getCompass(
            String groupName, String metricName, MetricLevel metricLevel, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getCompass(
                groupName, generateMetricName(metricName, metricLevel, tags));
    }

    /**
     * Get {@link FastCompass}. This metric is used when recording efficient statistical throughput,
     * average RT and metric of custom dimensions.
     */
    public static FastCompass getFastCompass(String groupName, String metricName, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getFastCompass(groupName, generateMetricName(metricName, tags));
    }

    /**
     * Get {@link FastCompass}. This metric is used when recording efficient statistical throughput,
     * average RT and metric of custom dimensions.
     */
    public static FastCompass getFastCompass(
            String groupName, String metricName, MetricLevel metricLevel, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        return MetricManager.getFastCompass(
                groupName, generateMetricName(metricName, metricLevel, tags));
    }

    /**
     * To get a {@link Gauge} metric, please register a Gauge metric to {@link MetricManager}. For
     * example:
     *
     * <p>Gauge< Integer> listenerSizeGauge = new Gauge< Integer>() { @Override public Integer
     * getValue() { return defaultEnv.getAllListeners().size(); } };
     * MetricManager.register("testGroup", MetricName.build("abc.defaultEnv.listenerSize"),
     * listenerSizeGauge);
     */
    public static void registerMetric(
            String groupName, String metricName, Metric metric, String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        try {
            MetricManager.register(groupName, generateMetricName(metricName, tags), metric);
        } catch (Exception exception) {
            LOG.warn("Failed to register metric " + metricName, exception);
        }
    }

    public static void registerMetric(
            String groupName,
            String metricName,
            Metric metric,
            MetricLevel metricLevel,
            String... tags) {
        checkNotNull(groupName);
        checkNotNull(metricName);

        try {
            MetricManager.register(
                    groupName, generateMetricName(metricName, metricLevel, tags), metric);
        } catch (Exception exception) {
            LOG.warn("Failed to register metric " + metricName, exception);
        }
    }

    private static MetricName generateMetricName(String metricName, String... tags) {
        return generateMetricName(metricName, null, tags);
    }

    private static MetricName generateMetricName(
            String metricName, MetricLevel metricLevel, String... tags) {
        MetricName buildName = MetricName.build(metricName);
        if (metricLevel != null) {
            buildName.level(metricLevel);
        }
        if (tags != null && tags.length > 0) {
            buildName.tagged(tags);
        }
        return buildName;
    }
}
