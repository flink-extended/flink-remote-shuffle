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

import com.alibaba.flink.shuffle.common.config.ConfigConstants;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.DelegatingConfiguration;
import com.alibaba.flink.shuffle.core.config.MetricOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Encapsulates everything needed for the instantiation and configuration of a metric reporter. */
public final class ReporterSetup {
    private static final Logger LOG = LoggerFactory.getLogger(ReporterSetup.class);
    // regex pattern to split the defined reporters
    private static final Pattern reporterListPattern = Pattern.compile("\\s*,\\s*");
    private static final String CONFIGURATION_ARGS_DELIMITER = ";";

    // regex pattern to extract the name from reporter configuration keys, e.g. "rep" from
    // "remote-shuffle.metrics.reporter.rep.class"
    private static final Pattern reporterClassPattern =
            Pattern.compile(
                    Pattern.quote(ConfigConstants.METRICS_REPORTER_PREFIX)
                            +
                            // [\S&&[^.]] = intersection of non-whitespace and non-period character
                            // classes
                            "([\\S&&[^.]]*)\\."
                            + '('
                            + Pattern.quote(ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX)
                            + '|'
                            + Pattern.quote(ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX)
                            + ')');

    public static void fromConfiguration(final Configuration conf) {
        String includedReportersString = conf.getString(MetricOptions.REPORTERS_LIST, "");
        Set<String> namedReporters =
                findEnabledReportersInConfiguration(conf, includedReportersString);

        if (namedReporters.isEmpty()) {
            LOG.info("Metric reporter factories are not configured");
            return;
        }

        final Map<String, Configuration> reporterFactories =
                loadAvailableReporterFactories(namedReporters, conf);

        reporterFactories.forEach(ReporterSetup::setupReporterViaReflection);
    }

    /**
     * @param namedReporters
     * @param conf
     * @return
     */
    private static Map<String, Configuration> loadAvailableReporterFactories(
            Set<String> namedReporters, Configuration conf) {
        Map<String, Configuration> availableReporterFactories = new HashMap<>();
        for (String namedReporter : namedReporters) {
            DelegatingConfiguration delegatingConfiguration =
                    new DelegatingConfiguration(
                            conf, ConfigConstants.METRICS_REPORTER_PREFIX + namedReporter + '.');

            final String reporterClassName =
                    delegatingConfiguration.getString(
                            ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, null);
            availableReporterFactories.put(reporterClassName, delegatingConfiguration);
        }
        return availableReporterFactories;
    }

    private static void setupReporterViaReflection(
            final String reporterFactory, final Configuration conf) {
        try {
            loadViaReflection(reporterFactory, conf);
        } catch (Throwable th) {
            LOG.error("Setup reporter " + reporterFactory + " error, ", th);
        }
    }

    /** This method is used for unit testing, so package level permissions are required. */
    static void loadViaReflection(final String reporterFactory, final Configuration conf)
            throws Exception {
        Class factoryClazz = Class.forName(reporterFactory);
        MetricReporterFactory metricReporterFactory =
                (MetricReporterFactory) factoryClazz.newInstance();
        metricReporterFactory.createMetricReporter(conf.toProperties());
        LOG.info("Setup metric reporter " + reporterFactory + " successfully");
    }

    private static Set<String> findEnabledReportersInConfiguration(
            Configuration configuration, String includedReportersString) {
        Set<String> includedReporters =
                reporterListPattern
                        .splitAsStream(includedReportersString)
                        .filter(r -> !r.isEmpty()) // splitting an empty string results in
                        // an empty string on jdk9+
                        .collect(Collectors.toSet());

        // use a TreeSet to make the reporter order deterministic, which is useful for testing
        Set<String> namedOrderedReporters = new TreeSet<>(String::compareTo);

        // scan entire configuration for keys starting with METRICS_REPORTER_PREFIX and determine
        // the set of enabled reporters
        for (String key : configuration.toMap().keySet()) {
            if (key.startsWith(ConfigConstants.METRICS_REPORTER_PREFIX)) {
                Matcher matcher = reporterClassPattern.matcher(key);
                if (matcher.matches()) {
                    String reporterName = matcher.group(1);
                    if (includedReporters.isEmpty() || includedReporters.contains(reporterName)) {
                        if (namedOrderedReporters.contains(reporterName)) {
                            LOG.warn(
                                    "Duplicate class configuration detected for reporter {}.",
                                    reporterName);
                        } else {
                            namedOrderedReporters.add(reporterName);
                        }
                    } else {
                        LOG.info(
                                "Excluding reporter {}, not configured in reporter list ({}).",
                                reporterName,
                                includedReportersString);
                    }
                }
            }
        }
        return namedOrderedReporters;
    }
}
