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

package com.alibaba.flink.shuffle.kubernetes.operator.resources;

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesConfigMapParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

/** Kubernetes ConfigMap builder. */
public class KubernetesConfigMapBuilder
        implements KubernetesResourceBuilder<ConfigMap, KubernetesConfigMapParameters> {

    @Override
    public ConfigMap buildKubernetesResourceFrom(
            KubernetesConfigMapParameters configMapParameters) {
        return new ConfigMapBuilder()
                .withApiVersion(Constants.API_VERSION)
                .editOrNewMetadata()
                .withName(configMapParameters.getConfigMapName())
                .withNamespace(configMapParameters.getNamespace())
                .withLabels(configMapParameters.getLabels())
                .endMetadata()
                .withData(configMapParameters.getData())
                .build();
    }
}
