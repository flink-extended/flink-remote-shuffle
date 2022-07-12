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

import com.alibaba.flink.shuffle.common.config.ConfigOption;

import java.time.Duration;

/** Config options for the {@link PrometheusReporter}. */
public class PrometheusReporterOptions {

    public static final ConfigOption<String> COMMON_TAGS =
            new ConfigOption<String>("remote-shuffle.metrics.reporter.prom.common-tags")
                    .defaultValue("")
                    .description("Specifies the common tags for all metrics.");

    public static final ConfigOption<Duration> INTERVAL =
            new ConfigOption<Duration>("remote-shuffle.metrics.reporter.prom.interval")
                    .defaultValue(Duration.ofSeconds(10))
                    .description(
                            "The interval between reports. This is used as a suffix in an actual reporter config.");

    public static final ConfigOption<String> PORTS =
            new ConfigOption<String>("remote-shuffle.metrics.reporter.prom.ports")
                    .defaultValue("9349-9359")
                    .description("The ports for PrometheusServer in pull mode.");
}
