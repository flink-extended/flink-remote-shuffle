/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDeploymentParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;

import java.util.Map;

/** Kubernetes Deployment builder. */
public class KubernetesDeploymentBuilder
        implements KubernetesResourceBuilder<Deployment, KubernetesDeploymentParameters> {
    @Override
    public Deployment buildKubernetesResourceFrom(
            KubernetesDeploymentParameters deploymentParameters) {

        final Pod resolvedPod =
                new KubernetesPodBuilder()
                        .buildKubernetesResourceFrom(
                                deploymentParameters.getPodTemplateParameters());

        final Map<String, String> labels = resolvedPod.getMetadata().getLabels();

        return new DeploymentBuilder()
                .withApiVersion(Constants.APPS_API_VERSION)
                .editOrNewMetadata()
                .withName(deploymentParameters.getDeploymentName())
                .withNamespace(deploymentParameters.getNamespace())
                .withLabels(deploymentParameters.getLabels())
                .endMetadata()
                .editOrNewSpec()
                .withReplicas(deploymentParameters.getReplicas())
                .withTemplate(getPodTemplate(resolvedPod))
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
