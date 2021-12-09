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

package com.alibaba.flink.shuffle.common.config;

/** ConfigConstants. */
public class ConfigConstants {
    /**
     * The prefix for per-reporter configs. Has to be combined with a reporter name and the configs
     * mentioned below.
     */
    public static final String METRICS_REPORTER_PREFIX = "remote-shuffle.metrics.reporter.";

    /** The class of the reporter to use. This is used as a suffix in an actual reporter config */
    public static final String METRICS_REPORTER_CLASS_SUFFIX = "class";

    /**
     * The class of the reporter factory to use. This is used as a suffix in an actual reporter
     * config
     */
    public static final String METRICS_REPORTER_FACTORY_CLASS_SUFFIX = "factory.class";
}
