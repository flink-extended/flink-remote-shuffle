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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.flink.shuffle.core.config.MetricOptions.METRICS_REPORTER_CLASSES;

/** Encapsulates everything needed for the instantiation and configuration of a metric reporter. */
public final class ReporterSetup {
    private static final Logger LOG = LoggerFactory.getLogger(ReporterSetup.class);

    private static final String CONFIGURATION_ARGS_DELIMITER = ";";

    public static void fromConfiguration(final Configuration conf) {
        String reportersString = conf.getString(METRICS_REPORTER_CLASSES);
        if (reportersString == null) {
            LOG.info("Metric reporter factories are not configured");
            return;
        }

        Set<String> reporterFactories =
                Stream.of(reportersString.split(CONFIGURATION_ARGS_DELIMITER))
                        .collect(Collectors.toSet());
        reporterFactories.forEach(
                factoryClass -> setupReporterViaReflection(factoryClass.trim(), conf));
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
}
