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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The configuration files will be deployed as a kubernetes {@link ConfigMap} and mounted to the
 * pod. This class is responsible for converting config files into kubernetes {@link ConfigMap}
 * configurations.
 */
public class K8sRemoteShuffleFileConfigsParameters implements KubernetesConfigMapParameters {
    private final String namespace;
    private final String clusterId;
    private final Map<String, String> data;

    public K8sRemoteShuffleFileConfigsParameters(
            String namespace, String clusterId, Map<String, String> data) {
        this.namespace = namespace;
        this.clusterId = clusterId;
        this.data = data;
    }

    @Override
    public String getConfigMapName() {
        return clusterId + "-configmap";
    }

    @Override
    public Map<String, String> getData() {
        return data;
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(KubernetesUtils.getCommonLabels(clusterId));
        return Collections.unmodifiableMap(labels);
    }
}
