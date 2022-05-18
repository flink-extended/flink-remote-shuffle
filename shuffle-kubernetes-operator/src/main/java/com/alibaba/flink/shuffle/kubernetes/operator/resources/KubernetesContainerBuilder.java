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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesContainerParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** Kubernetes Container builder. */
public class KubernetesContainerBuilder
        implements KubernetesResourceBuilder<Container, KubernetesContainerParameters> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesContainerBuilder.class);

    @Override
    public Container buildKubernetesResourceFrom(
            KubernetesContainerParameters containerParameters) {

        final ResourceRequirements requirements =
                KubernetesUtils.getResourceRequirements(
                        containerParameters.getContainerMemoryMB(),
                        containerParameters.getContainerCPU());
        KubernetesUtils.updateResourceRequirements(
                requirements, containerParameters.getResourceLimitFactors());

        ContainerCommandAndArgs commandAndArgs = containerParameters.getContainerCommandAndArgs();

        return new ContainerBuilder()
                .withName(containerParameters.getContainerName())
                .withImage(containerParameters.getContainerImage())
                .withImagePullPolicy(containerParameters.getContainerImagePullPolicy())
                .withCommand(commandAndArgs.getCommand())
                .withArgs(commandAndArgs.getArgs())
                .withResources(requirements)
                .withVolumeMounts(getVolumeMounts(containerParameters))
                .withEnv(getEnvironmentVars(containerParameters))
                .build();
    }

    private List<VolumeMount> getVolumeMounts(KubernetesContainerParameters containerParameters) {
        return containerParameters.getContainerVolumeMounts().stream()
                .map(this::getVolumeMount)
                .collect(Collectors.toList());
    }

    private VolumeMount getVolumeMount(Map<String, String> stringMap) {
        checkState(stringMap.containsKey(Constants.VOLUME_NAME));
        checkState(stringMap.containsKey(Constants.VOLUME_MOUNT_PATH));

        final VolumeMountBuilder volumeMountBuilder = new VolumeMountBuilder();
        stringMap.forEach(
                (k, v) -> {
                    switch (k) {
                        case Constants.VOLUME_NAME:
                            volumeMountBuilder.withName(v);
                            break;
                        case Constants.VOLUME_MOUNT_PATH:
                            volumeMountBuilder.withMountPath(v);
                            break;
                        default:
                            LOG.warn("Unrecognized key({}) of volume mount, will ignore.", k);
                            break;
                    }
                });

        return volumeMountBuilder.build();
    }

    private List<EnvVar> getEnvironmentVars(KubernetesContainerParameters containerParameters) {
        List<EnvVar> envVars =
                containerParameters.getEnvironmentVars().entrySet().stream()
                        .map(
                                kv ->
                                        new EnvVarBuilder()
                                                .withName(kv.getKey())
                                                .withValue(kv.getValue())
                                                .build())
                        .collect(Collectors.toList());

        Map<String, String> fieldRefEnvVars = new HashMap<>();
        fieldRefEnvVars.put(
                Constants.ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS, Constants.POD_IP_FIELD_PATH);
        fieldRefEnvVars.put(Constants.ENV_REMOTE_SHUFFLE_POD_NAME, Constants.POD_NAME_FIELD_PATH);
        envVars.addAll(
                fieldRefEnvVars.entrySet().stream()
                        .map(
                                kv ->
                                        new EnvVarBuilder()
                                                .withName(kv.getKey())
                                                .withValueFrom(
                                                        new EnvVarSourceBuilder()
                                                                .withNewFieldRef(
                                                                        Constants.API_VERSION,
                                                                        kv.getValue())
                                                                .build())
                                                .build())
                        .collect(Collectors.toList()));

        return envVars;
    }
}
