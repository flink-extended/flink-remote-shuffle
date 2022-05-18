/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

package com.alibaba.flink.shuffle.coordinator.metrics;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.rest.RestHandler;
import com.alibaba.flink.shuffle.rest.RestResponse;

import com.alibaba.metrics.ClusterHistogram;
import com.alibaba.metrics.Compass;
import com.alibaba.metrics.Counter;
import com.alibaba.metrics.FastCompass;
import com.alibaba.metrics.Gauge;
import com.alibaba.metrics.Histogram;
import com.alibaba.metrics.IMetricManager;
import com.alibaba.metrics.Meter;
import com.alibaba.metrics.MetricManager;
import com.alibaba.metrics.MetricName;
import com.alibaba.metrics.MetricRegistry;
import com.alibaba.metrics.Timer;
import com.alibaba.metrics.common.CollectLevel;
import com.alibaba.metrics.common.MetricObject;
import com.alibaba.metrics.common.MetricsCollector;
import com.alibaba.metrics.common.MetricsCollectorFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * A rest handler for metrics request.
 *
 * <p>This class borrows code from com.alibaba.metrics.rest.MetricsResource.
 */
public class MetricsRestHandler implements RestHandler {

    public static final String QUERY_PATH = "/metrics";

    public static final String PARAM_GROUP = "group";

    public static final String ALL_REMOTE_SHUFFLE = "remote-shuffle";

    public static final String REMOTE_SHUFFLE_PREFIX = "remote-shuffle.";

    @Override
    public RestResponse handle(Map<String, String[]> parameters) {
        // currently, we support all metrics and group metrics query
        IMetricManager metricManager = MetricManager.getIMetricManager();
        CommonUtils.checkState(metricManager.isEnabled(), "Metrics are not enabled.");

        List<String> allGroups = metricManager.listMetricGroups();
        final Set<String> distinctGroups = new HashSet<>();
        String[] queryGroups = parameters.get(PARAM_GROUP);

        if (queryGroups != null) {
            distinctGroups.addAll(Arrays.asList(queryGroups));
        }

        if (distinctGroups.contains(ALL_REMOTE_SHUFFLE)) {
            distinctGroups.remove(ALL_REMOTE_SHUFFLE);
            allGroups.stream()
                    .filter(group -> group.startsWith(REMOTE_SHUFFLE_PREFIX))
                    .forEach(distinctGroups::add);
        }

        Map<String, List<MetricObject>> metricsData = new TreeMap<>();
        if (distinctGroups.isEmpty()) {
            distinctGroups.addAll(allGroups);
        }

        for (String queryGroup : distinctGroups) {
            if (allGroups.contains(queryGroup)) {
                MetricRegistry metricRegistry = metricManager.getMetricRegistryByGroup(queryGroup);
                metricsData.put(queryGroup, buildMetrics(metricRegistry));
            }
        }

        return new MetricResponse(metricsData);
    }

    @Override
    public String getQueryPath() {
        return QUERY_PATH;
    }

    private List<MetricObject> buildMetrics(MetricRegistry registry) {
        long currentTime = System.currentTimeMillis();
        MetricsCollector collector =
                MetricsCollectorFactory.createNew(
                        CollectLevel.NORMAL,
                        TimeUnit.SECONDS.toSeconds(1),
                        1.0 / TimeUnit.MILLISECONDS.toNanos(1),
                        null);

        SortedMap<MetricName, Gauge> gauges = registry.getGauges();
        for (Map.Entry<MetricName, Gauge> entry : gauges.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, Counter> counters = registry.getCounters();
        for (Map.Entry<MetricName, Counter> entry : counters.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, Meter> meters = registry.getMeters();
        for (Map.Entry<MetricName, Meter> entry : meters.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, Histogram> histograms = registry.getHistograms();
        for (Map.Entry<MetricName, Histogram> entry : histograms.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, Timer> timers = registry.getTimers();
        for (Map.Entry<MetricName, Timer> entry : timers.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, Compass> compasses = registry.getCompasses();
        for (Map.Entry<MetricName, Compass> entry : compasses.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, FastCompass> fastCompasses = registry.getFastCompasses();
        for (Map.Entry<MetricName, FastCompass> entry : fastCompasses.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        SortedMap<MetricName, ClusterHistogram> clusterHistograms = registry.getClusterHistograms();
        for (Map.Entry<MetricName, ClusterHistogram> entry : clusterHistograms.entrySet()) {
            collector.collect(entry.getKey(), entry.getValue(), currentTime);
        }

        return collector.build();
    }
}
