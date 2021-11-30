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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters.util;

import java.util.Map;

/** A utility class that contains the parameters for constructing a kubernetes configmap. */
public class ConfigMapVolume {

    private final String configMapName;
    private final String volumeName;
    private final Map<String, String> items;
    private final String mountPath;

    public ConfigMapVolume(
            String volumeName, String configMapName, Map<String, String> items, String mountPath) {
        this.volumeName = volumeName;
        this.configMapName = configMapName;
        this.items = items;
        this.mountPath = mountPath;
    }

    public String getConfigMapName() {
        return configMapName;
    }

    public Map<String, String> getItems() {
        return items;
    }

    public String getVolumeName() {
        return volumeName;
    }

    public String getMountPath() {
        return mountPath;
    }
}
