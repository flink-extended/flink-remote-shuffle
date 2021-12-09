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

package com.alibaba.flink.shuffle.core.config;

import com.alibaba.flink.shuffle.common.config.ConfigOption;

/** Config options for metrics. */
public class MetricOptions {
    /**
     * An optional list of reporter names. If configured, only reporters whose name matches any of
     * the names in the list will be started. Otherwise, all reporters that could be found in the
     * configuration will be started.
     *
     * <p>Example:
     *
     * <pre>{@code
     * metrics.reporters = foo,bar
     *
     * metrics.reporter.foo.class = org.apache.flink.metrics.reporter.JMXReporter
     * metrics.reporter.foo.interval = 10
     *
     * metrics.reporter.bar.class = org.apache.flink.metrics.graphite.GraphiteReporter
     * metrics.reporter.bar.port = 1337
     * }</pre>
     */
    public static final ConfigOption<String> REPORTERS_LIST =
            new ConfigOption<String>("remote-shuffle.metrics.reporter")
                    .description(
                            "An optional list of reporter names. If configured, only reporters whose name matches"
                                    + " any of the names in the list will be started. Otherwise, all reporters that could be found in"
                                    + " the configuration will be started.");

    /** Whether the http server for reading metrics is enabled. */
    public static final ConfigOption<Boolean> METRICS_HTTP_SERVER_ENABLE =
            new ConfigOption<Boolean>("remote-shuffle.metrics.enabled-http-server")
                    .defaultValue(true)
                    .description("Whether the http server for requesting metrics is enabled.");

    /** The local address of the network interface that the http metric server binds to. */
    public static final ConfigOption<String> METRICS_BIND_HOST =
            new ConfigOption<String>("remote-shuffle.metrics.bind-host")
                    .defaultValue("0.0.0.0")
                    .description(
                            "The local address of the network interface that the http metric server"
                                    + " binds to.");

    /** Shuffle manager http metric server bind port. */
    public static final ConfigOption<Integer> METRICS_SHUFFLE_MANAGER_HTTP_BIND_PORT =
            new ConfigOption<Integer>("remote-shuffle.metrics.manager.bind-port")
                    .defaultValue(23101)
                    .description("Shuffle manager http metric server bind port.");

    /** Shuffle worker http metric server bind port. */
    public static final ConfigOption<Integer> METRICS_SHUFFLE_WORKER_HTTP_BIND_PORT =
            new ConfigOption<Integer>("remote-shuffle.metrics.worker.bind-port")
                    .defaultValue(23103)
                    .description("Shuffle worker http metric server bind port.");

    /**
     * Specify the implementation classes of metrics reporter. Separate by ';' if there are multiple
     * class names. Each class name needs a package name prefix, e.g. a.b.c.Factory1;a.b.c.Factory2.
     */
    public static final ConfigOption<String> METRICS_REPORTER_CLASSES =
            new ConfigOption<String>("remote-shuffle.metrics.reporter.factories")
                    .defaultValue(null)
                    .description(
                            "Specify the implementation classes of metrics reporter. Separate by "
                                    + "';' if there are multiple class names. Each class name needs"
                                    + " a package name prefix, e.g. a.b.c.Factory1;a.b.c.Factory2.");
    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private MetricOptions() {}
}
