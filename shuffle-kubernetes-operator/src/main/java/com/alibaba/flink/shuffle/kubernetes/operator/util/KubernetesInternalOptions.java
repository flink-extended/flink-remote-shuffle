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

package com.alibaba.flink.shuffle.kubernetes.operator.util;

import com.alibaba.flink.shuffle.common.config.ConfigOption;

import java.util.Collections;
import java.util.Map;

/** Kubernetes configuration options that are not meant to be set by the user. */
public class KubernetesInternalOptions {

    // --------------------------------------------------------------------------------------------
    //  Internal configurations.
    // --------------------------------------------------------------------------------------------
    public static final ConfigOption<String> NAMESPACE =
            new ConfigOption<String>("remote-shuffle.kubernetes.namespace")
                    .defaultValue("default")
                    .description(
                            "The namespace that will be used for running the shuffle manager and "
                                    + "worker pods.");

    public static final ConfigOption<String> CONFIG_VOLUME_NAME =
            new ConfigOption<String>("remote-shuffle.kubernetes.config-volume.name")
                    .defaultValue(null)
                    .description("Config volume name.");

    public static final ConfigOption<String> CONFIG_VOLUME_CONFIG_MAP_NAME =
            new ConfigOption<String>("remote-shuffle.kubernetes.config-volume.config-map-name")
                    .defaultValue(null)
                    .description("Config map name.");

    public static final ConfigOption<Map<String, String>> CONFIG_VOLUME_ITEMS =
            new ConfigOption<Map<String, String>>("remote-shuffle.kubernetes.config-volume.items")
                    .defaultValue(Collections.emptyMap())
                    .description("Config volume items.");

    public static final ConfigOption<String> CONFIG_VOLUME_MOUNT_PATH =
            new ConfigOption<String>("remote-shuffle.kubernetes.config-volume.mount-path")
                    .defaultValue(null)
                    .description("Config volume mount path.");
}
