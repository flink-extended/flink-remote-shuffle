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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesStatefulSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetUpdateStrategyBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Builder for StatefulSet. */
public class KubernetesStatefulSetBuilder
        implements KubernetesResourceBuilder<StatefulSet, KubernetesStatefulSetParameters> {

    private static final String PVC_NAME = "shuffle-data";
    public static final KubernetesStatefulSetBuilder INSTANCE = new KubernetesStatefulSetBuilder();

    @Override
    public StatefulSet buildKubernetesResourceFrom(KubernetesStatefulSetParameters parameters) {
        final Pod resolvedPod =
                new KubernetesPodBuilder()
                        .buildKubernetesResourceFrom(parameters.getPodTemplateParameters());

        injectPVCVolumeMounts(resolvedPod, parameters);
        final Map<String, String> labels = resolvedPod.getMetadata().getLabels();

        return new StatefulSetBuilder()
                .withApiVersion(Constants.APPS_API_VERSION)
                .editOrNewMetadata()
                .withName(parameters.getStatefulSetName())
                .withNamespace(parameters.getNamespace())
                .withLabels(parameters.getLabels())
                .endMetadata()
                .editOrNewSpec()
                .withServiceName(getServiceName(parameters))
                .withTemplate(getPodTemplate(resolvedPod))
                .withUpdateStrategy(
                        new StatefulSetUpdateStrategyBuilder()
                                .withType(Constants.ROLLING_UPDATE)
                                .build())
                .editOrNewSelector()
                .addToMatchLabels(labels)
                .endSelector()
                .withReplicas(parameters.getReplicas())
                .addNewVolumeClaimTemplate()
                .editOrNewMetadata()
                .withName(getVolumeMountName(parameters.getStatefulSetName()))
                .withLabels(labels)
                .withNamespace(parameters.getNamespace())
                .endMetadata()
                .editOrNewSpec()
                .withStorageClassName(parameters.getStorageClass())
                .withAccessModes(parameters.getAccessMode())
                .withResources(
                        new ResourceRequirementsBuilder()
                                .withRequests(parameters.getStorageResource())
                                .withLimits(parameters.getStorageResource())
                                .build())
                .endSpec()
                .endVolumeClaimTemplate()
                .endSpec()
                .build();
    }

    public PodTemplateSpec getPodTemplate(Pod resolvedPod) {
        return new PodTemplateSpecBuilder()
                .withMetadata(resolvedPod.getMetadata())
                .withSpec(resolvedPod.getSpec())
                .build();
    }

    private String getServiceName(KubernetesStatefulSetParameters parameters) {
        return String.format("%s-service", parameters.getStatefulSetName());
    }

    private void injectPVCVolumeMounts(
            Pod resolvedPod, KubernetesStatefulSetParameters parameters) {
        List<Container> containersList =
                resolvedPod.getSpec().getContainers().stream()
                        .filter(
                                c ->
                                        c.getName()
                                                .equals(
                                                        KubernetesUtils
                                                                .SHUFFLE_WORKER_CONTAINER_NAME))
                        .collect(Collectors.toList());
        // must be 1;
        CommonUtils.checkArgument(
                containersList.size() == 1,
                String.format(
                        "The target container is expect equal to 1, but get %s",
                        containersList.size()));
        Container container = containersList.get(0);
        container
                .getVolumeMounts()
                .add(
                        new VolumeMountBuilder()
                                .withName(getVolumeMountName(parameters.getStatefulSetName()))
                                .withMountPath(parameters.getMountPath())
                                .build());
    }

    private String getVolumeMountName(String statefulSetName) {
        return String.format("%s-%s", statefulSetName, PVC_NAME);
    }
}
