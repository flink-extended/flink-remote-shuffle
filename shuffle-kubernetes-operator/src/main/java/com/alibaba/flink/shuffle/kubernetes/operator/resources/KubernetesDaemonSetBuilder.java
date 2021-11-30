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

package com.alibaba.flink.shuffle.kubernetes.operator.resources;

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDaemonSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import io.fabric8.kubernetes.api.model.apps.DaemonSetBuilder;
import io.fabric8.kubernetes.api.model.apps.DaemonSetUpdateStrategyBuilder;

import java.util.Map;

/** Kubernetes DaemonSet builder. */
public class KubernetesDaemonSetBuilder
        implements KubernetesResourceBuilder<DaemonSet, KubernetesDaemonSetParameters> {

    @Override
    public DaemonSet buildKubernetesResourceFrom(
            KubernetesDaemonSetParameters daemonSetParameters) {

        final Pod resolvedPod =
                new KubernetesPodBuilder()
                        .buildKubernetesResourceFrom(
                                daemonSetParameters.getPodTemplateParameters());

        final Map<String, String> labels = resolvedPod.getMetadata().getLabels();

        return new DaemonSetBuilder()
                .withApiVersion(Constants.APPS_API_VERSION)
                .editOrNewMetadata()
                .withName(daemonSetParameters.getDaemonSetName())
                .withNamespace(daemonSetParameters.getNamespace())
                .withLabels(daemonSetParameters.getLabels())
                .endMetadata()
                .editOrNewSpec()
                .withTemplate(getPodTemplate(resolvedPod))
                .withUpdateStrategy(
                        new DaemonSetUpdateStrategyBuilder()
                                .withType(Constants.ROLLING_UPDATE)
                                .build())
                .editOrNewSelector()
                .addToMatchLabels(labels)
                .endSelector()
                .endSpec()
                .build();
    }

    public PodTemplateSpec getPodTemplate(Pod resolvedPod) {
        return new PodTemplateSpecBuilder()
                .withMetadata(resolvedPod.getMetadata())
                .withSpec(resolvedPod.getSpec())
                .build();
    }
}
