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

import com.alibaba.flink.shuffle.common.config.ConfigOption;

import java.time.Duration;

/** Config options for the {@link PrometheusPushGatewayReporter}. */
public class PrometheusPushGatewayReporterOptions {
    public static final ConfigOption<String> HOST =
            new ConfigOption<String>("host")
                    .defaultValue(null)
                    .description("Whether the http server for requesting metrics is enabled.");

    public static final ConfigOption<Integer> PORT =
            new ConfigOption<Integer>("port")
                    .defaultValue(-1)
                    .description("The PushGateway server port.");

    public static final ConfigOption<String> JOB_NAME =
            new ConfigOption<String>("jobName")
                    .defaultValue("flink-remote-shuffle")
                    .description("The job name under which metrics will be pushed");

    public static final ConfigOption<Boolean> RANDOM_JOB_NAME_SUFFIX =
            new ConfigOption<Boolean>("randomJobNameSuffix")
                    .defaultValue(true)
                    .description(
                            "Specifies whether a random suffix should be appended to the job name.");

    public static final ConfigOption<Boolean> DELETE_ON_SHUTDOWN =
            new ConfigOption<Boolean>("deleteOnShutdown")
                    .defaultValue(true)
                    .description(
                            "Specifies whether to delete metrics from the PushGateway on shutdown."
                                    + " Flink will try its best to delete the metrics but this is not guaranteed. See %s for more details.");

    public static final ConfigOption<Boolean> FILTER_LABEL_VALUE_CHARACTER =
            new ConfigOption<Boolean>("filterLabelValueCharacters")
                    .defaultValue(true)
                    .description(
                            "Specifies whether to filter label value characters."
                                    + " If enabled, all characters not matching [a-zA-Z0-9:_] will be removed,"
                                    + " otherwise no characters will be removed.");

    public static final ConfigOption<String> GROUPING_KEY =
            new ConfigOption<String>("groupingKey")
                    .defaultValue("")
                    .description(
                            "Specifies the grouping key which is the group and global labels of all metrics.");

    public static final ConfigOption<Duration> INTERVAL =
            new ConfigOption<Duration>("interval")
                    .defaultValue(Duration.ofMinutes(1))
                    .description(
                            "The interval between reports. This is used as a suffix in an actual reporter config.");
}
