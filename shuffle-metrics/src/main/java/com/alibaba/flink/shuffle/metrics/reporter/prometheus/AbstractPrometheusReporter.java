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
import com.alibaba.metrics.Snapshot;
import com.alibaba.metrics.Timer;
import com.alibaba.metrics.common.config.MetricsCollectPeriodConfig;
import com.alibaba.metrics.reporter.MetricManagerReporter;
import io.prometheus.client.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/** Base prometheus reporter for prometheus metrics. */
public class AbstractPrometheusReporter extends MetricManagerReporter {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractPrometheusReporter.class);

    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    private final Map<String, String> commonTags;

    private final Map<String, AbstractMap.SimpleImmutableEntry<Collector, Integer>>
            collectorsWithCountByMetricName = new ConcurrentHashMap<>();

    public AbstractPrometheusReporter(
            IMetricManager metricManager,
            String name,
            MetricFilter filter,
            Map<String, String> commonTags,
            MetricsCollectPeriodConfig metricsReportPeriodConfig,
            TimeUnit rateUnit,
            TimeUnit durationUnit) {
        super(metricManager, name, filter, metricsReportPeriodConfig, rateUnit, durationUnit);
        this.commonTags = commonTags;
    }

    @Override
    public void report(
            Map<MetricName, Gauge> gauges,
            Map<MetricName, Counter> counters,
            Map<MetricName, Histogram> histograms,
            Map<MetricName, Meter> meters,
            Map<MetricName, Timer> timers,
            Map<MetricName, Compass> compasses,
            Map<MetricName, FastCompass> fastCompasses,
            Map<MetricName, ClusterHistogram> clusterHistogrames) {

        Arrays.asList(
                        gauges,
                        counters,
                        histograms,
                        meters,
                        timers,
                        compasses,
                        fastCompasses,
                        clusterHistogrames)
                .forEach(this::reportMetrics);
    }

    private void reportMetrics(Map<MetricName, ? extends Metric> metricMap) {
        metricMap.forEach(this::sinkIntoCollector);
    }

    private void sinkIntoCollector(MetricName metricName, Metric metric) {

        String validMetricName = filterCharacters(metricName.getKey());

        List<String> dimensionKeys = new LinkedList<>();
        List<String> dimensionValues = new LinkedList<>();

        for (final Map.Entry<String, String> tagEntry : commonTags.entrySet()) {
            dimensionKeys.add(tagEntry.getKey());
            dimensionValues.add(tagEntry.getValue());
        }

        for (final Map.Entry<String, String> dimension : metricName.getTags().entrySet()) {
            dimensionKeys.add(filterCharacters(dimension.getKey()));
            dimensionValues.add(filterCharacters(dimension.getValue()));
        }
        final Collector collector;
        int count = 0;
        if (collectorsWithCountByMetricName.containsKey(validMetricName)) {
            final AbstractMap.SimpleImmutableEntry<Collector, Integer> collectorWithCount =
                    collectorsWithCountByMetricName.get(validMetricName);
            collector = collectorWithCount.getKey();
            count = collectorWithCount.getValue();
        } else {
            collector =
                    createCollector(
                            metric,
                            dimensionKeys,
                            dimensionValues,
                            validMetricName,
                            String.format("HELP %s", metricName.getKey()));
            try {
                collector.register();
            } catch (Exception e) {
                LOG.warn("There was a problem registering metric {}.", validMetricName, e);
            }
        }
        addMetricToCollector(metric, dimensionValues, collector);
        collectorsWithCountByMetricName.put(
                validMetricName, new AbstractMap.SimpleImmutableEntry<>(collector, count + 1));
    }

    private Collector createCollector(
            Metric metric,
            List<String> dimensionKeys,
            List<String> dimensionValues,
            String scopedMetricName,
            String helpString) {
        Collector collector;
        if (metric instanceof Gauge || metric instanceof Counter || metric instanceof Meter) {
            collector =
                    io.prometheus.client.Gauge.build()
                            .name(scopedMetricName)
                            .help(helpString)
                            .labelNames(toArray(dimensionKeys))
                            .create();
        } else if (metric instanceof Histogram) {
            collector =
                    new HistogramSummaryProxy(
                            (Histogram) metric,
                            scopedMetricName,
                            helpString,
                            dimensionKeys,
                            dimensionValues);
        } else {
            LOG.warn(
                    "Cannot create collector for unknown metric type: {}. "
                            + "Metric types [Timer, Compass, FastCompass, ClusterHistogram] is not supported by this reporter.",
                    metric.getClass().getName());
            collector = null;
        }
        return collector;
    }

    static class HistogramSummaryProxy extends Collector {
        static final String DEFAULT_HELP_STRING = "";
        static final List<Double> QUANTILES = Arrays.asList(.5, .75, .95, .98, .99, .999);

        private final String metricName;
        private final String helpString;
        private final List<String> labelNamesWithQuantile;

        private final Map<List<String>, Histogram> histogramsByLabelValues = new HashMap<>();

        HistogramSummaryProxy(
                final Histogram histogram,
                final String metricName,
                final List<String> labelNames,
                final List<String> labelValues) {
            this(histogram, metricName, DEFAULT_HELP_STRING, labelNames, labelValues);
        }

        HistogramSummaryProxy(
                final Histogram histogram,
                final String metricName,
                final String helpString,
                final List<String> labelNames,
                final List<String> labelValues) {
            this.metricName = metricName;
            this.helpString = helpString;
            this.labelNamesWithQuantile = addToList(labelNames, "quantile");
            histogramsByLabelValues.put(labelValues, histogram);
        }

        @Override
        public List<MetricFamilySamples> collect() {
            // We cannot use SummaryMetricFamily because it is impossible to get a sum of all values
            // (at least for Dropwizard histograms,
            // whose snapshot's values array only holds a sample of recent values).

            List<MetricFamilySamples.Sample> samples = new LinkedList<>();
            for (Map.Entry<List<String>, Histogram> labelValuesToHistogram :
                    histogramsByLabelValues.entrySet()) {
                addSamples(
                        labelValuesToHistogram.getKey(),
                        labelValuesToHistogram.getValue(),
                        samples);
            }
            return Collections.singletonList(
                    new MetricFamilySamples(metricName, Type.SUMMARY, helpString, samples));
        }

        void addChild(final Histogram histogram, final List<String> labelValues) {
            histogramsByLabelValues.put(labelValues, histogram);
        }

        void remove(final List<String> labelValues) {
            histogramsByLabelValues.remove(labelValues);
        }

        private void addSamples(
                final List<String> labelValues,
                final Histogram histogram,
                final List<MetricFamilySamples.Sample> samples) {
            samples.add(
                    new MetricFamilySamples.Sample(
                            metricName + "_count",
                            labelNamesWithQuantile.subList(0, labelNamesWithQuantile.size() - 1),
                            labelValues,
                            histogram.getCount()));
            final Snapshot statistics = histogram.getSnapshot();

            for (final Double quantile : QUANTILES) {
                samples.add(
                        new MetricFamilySamples.Sample(
                                metricName,
                                labelNamesWithQuantile,
                                addToList(labelValues, quantile.toString()),
                                statistics.getValue(quantile)));
            }
        }
    }

    private static List<String> addToList(List<String> list, String element) {
        final List<String> result = new ArrayList<>(list);
        result.add(element);
        return result;
    }

    private static String[] toArray(List<String> list) {
        return list.toArray(new String[list.size()]);
    }

    private void addMetricToCollector(
            Metric metric, List<String> dimensionValues, Collector collector) {
        if (metric instanceof Gauge) {
            ((io.prometheus.client.Gauge) collector)
                    .setChild(gaugeFrom((Gauge) metric), toArray(dimensionValues));
        } else if (metric instanceof Counter) {
            ((io.prometheus.client.Gauge) collector)
                    .setChild(gaugeFrom((Counter) metric), toArray(dimensionValues));
        } else if (metric instanceof Meter) {
            ((io.prometheus.client.Gauge) collector)
                    .setChild(gaugeFrom((Meter) metric), toArray(dimensionValues));
        } else if (metric instanceof Histogram) {
            ((HistogramSummaryProxy) collector).addChild((Histogram) metric, dimensionValues);
        } else {
            LOG.warn(
                    "Cannot add unknown metric type: {}. This indicates that the metric type is not supported by this reporter.",
                    metric.getClass().getName());
        }
    }

    io.prometheus.client.Gauge.Child gaugeFrom(Gauge gauge) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                final Object value = gauge.getValue();
                if (value == null) {
                    LOG.debug("Gauge {} is null-valued, defaulting to 0.", gauge);
                    return 0;
                }
                if (value instanceof Double) {
                    return (double) value;
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                if (value instanceof Boolean) {
                    return ((Boolean) value) ? 1 : 0;
                }
                LOG.debug(
                        "Invalid type for Gauge {}: {}, only number types and booleans are supported by this reporter.",
                        gauge,
                        value.getClass().getName());
                return 0;
            }
        };
    }

    private static io.prometheus.client.Gauge.Child gaugeFrom(Counter counter) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                return (double) counter.getCount();
            }
        };
    }

    private static io.prometheus.client.Gauge.Child gaugeFrom(Meter meter) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                return meter.getOneMinuteRate();
            }
        };
    }

    private static String filterCharacters(String input) {
        // https://prometheus.io/docs/instrumenting/writing_exporters/
        // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
        // an underscore.
        return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }
}
