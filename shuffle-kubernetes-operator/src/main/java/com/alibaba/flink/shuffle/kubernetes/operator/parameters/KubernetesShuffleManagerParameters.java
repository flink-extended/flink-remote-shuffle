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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.core.config.memory.ShuffleManagerProcessSpec;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ConfigMapVolume;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.apps.Deployment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * ShuffleManager will be deployed as a {@link Deployment} in Kubernetes. This class helps to parse
 * ShuffleManger configuration to kubernetes {@link Deployment} configuration.
 */
public class KubernetesShuffleManagerParameters extends AbstractKubernetesParameters
        implements KubernetesDeploymentParameters {

    private final ShuffleManagerProcessSpec shuffleManagerProcessSpec;

    public KubernetesShuffleManagerParameters(Configuration conf) {
        super(conf);
        this.shuffleManagerProcessSpec = new ShuffleManagerProcessSpec(conf);
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(
                Optional.ofNullable(conf.getMap(KubernetesOptions.SHUFFLE_MANAGER_LABELS))
                        .orElse(Collections.emptyMap()));
        labels.putAll(KubernetesUtils.getCommonLabels(getClusterId()));
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_SHUFFLE_MANAGER);
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return Optional.ofNullable(conf.getMap(KubernetesOptions.SHUFFLE_MANAGER_NODE_SELECTOR))
                .orElse(Collections.emptyMap());
    }

    @Override
    public String getContainerName() {
        return KubernetesUtils.SHUFFLE_MANAGER_CONTAINER_NAME;
    }

    @Override
    public List<Map<String, String>> getEmptyDirVolumes() {
        return KubernetesUtils.filterVolumesConfigs(
                conf,
                KubernetesOptions.SHUFFLE_MANAGER_EMPTY_DIR_VOLUMES,
                KubernetesUtils::filterEmptyDirVolumeConfigs);
    }

    @Override
    public List<Map<String, String>> getHostPathVolumes() {
        return KubernetesUtils.filterVolumesConfigs(
                conf,
                KubernetesOptions.SHUFFLE_MANAGER_HOST_PATH_VOLUMES,
                KubernetesUtils::filterHostPathVolumeConfigs);
    }

    @Override
    public List<Map<String, String>> getTolerations() {
        return Optional.ofNullable(
                        conf.getList(KubernetesOptions.SHUFFLE_MANAGER_TOLERATIONS, Map.class))
                .orElse(Collections.emptyList());
    }

    @Override
    public Map<String, String> getEnvironmentVars() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(Constants.LABEL_APPTYPE_KEY.toUpperCase(), Constants.LABEL_APPTYPE_VALUE);
        envVars.put(Constants.LABEL_APP_KEY.toUpperCase(), getClusterId());
        envVars.put(
                Constants.LABEL_COMPONENT_KEY.toUpperCase(),
                Constants.LABEL_COMPONENT_SHUFFLE_MANAGER);
        envVars.putAll(conf.getMap(KubernetesOptions.SHUFFLE_MANAGER_ENV_VARS));
        return envVars;
    }

    @Override
    public List<Map<String, String>> getContainerVolumeMounts() {

        List<Map<String, String>> volumeMountsConfigs = new ArrayList<>();

        // empty dir volume mounts
        volumeMountsConfigs.addAll(
                KubernetesUtils.filterVolumesConfigs(
                        conf,
                        KubernetesOptions.SHUFFLE_MANAGER_EMPTY_DIR_VOLUMES,
                        KubernetesUtils::filterVolumeMountsConfigs));

        // host path volume mounts
        volumeMountsConfigs.addAll(
                KubernetesUtils.filterVolumesConfigs(
                        conf,
                        KubernetesOptions.SHUFFLE_MANAGER_HOST_PATH_VOLUMES,
                        KubernetesUtils::filterVolumeMountsConfigs));

        // configmap volume mounts
        Map<String, String> configmapVolumeMounts = new HashMap<>();
        List<ConfigMapVolume> configMapVolumes = getConfigMapVolumes();
        if (!configMapVolumes.isEmpty()) {
            checkState(configMapVolumes.size() == 1);
            ConfigMapVolume configMapVolume = configMapVolumes.get(0);
            configmapVolumeMounts.put(Constants.VOLUME_NAME, configMapVolume.getVolumeName());
            configmapVolumeMounts.put(Constants.VOLUME_MOUNT_PATH, configMapVolume.getMountPath());
            volumeMountsConfigs.add(configmapVolumeMounts);
        }

        return volumeMountsConfigs;
    }

    @Override
    public Integer getContainerMemoryMB() {
        return shuffleManagerProcessSpec.getTotalProcessMemorySize().getMebiBytes();
    }

    @Override
    public Double getContainerCPU() {
        return conf.getDouble(KubernetesOptions.SHUFFLE_MANAGER_CPU);
    }

    @Override
    public Map<String, String> getResourceLimitFactors() {
        return KubernetesUtils.getPrefixedKeyValuePairs(
                KubernetesOptions.SHUFFLE_MANAGER_RESOURCE_LIMIT_FACTOR_PREFIX, conf);
    }

    @Override
    public ContainerCommandAndArgs getContainerCommandAndArgs() {
        String command = "bash";
        return new ContainerCommandAndArgs(
                command,
                Arrays.asList(
                        "-c",
                        Constants.SHUFFLE_MANAGER_SCRIPT_PATH + " " + getShuffleManagerConfigs()));
    }

    @Override
    public String getDeploymentName() {
        // The shuffle manager deployment name pattern is {clusterId}-{shufflemanager}
        return KubernetesUtils.getShuffleManagerNameWithClusterId(getClusterId());
    }

    @Override
    public int getReplicas() {
        return 1;
    }

    @Override
    public KubernetesContainerParameters getContainerParameters() {
        return this;
    }

    @Override
    public KubernetesPodParameters getPodTemplateParameters() {
        return this;
    }

    private String getShuffleManagerConfigs() {
        List<String> dynamicConfigs = new ArrayList<>();
        conf.toMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(
                        kv -> {
                            String configKey = kv.getKey();
                            // following configs should not be passed to ShuffleManager:
                            // (1) start with "remote-shuffle.kubernetes".
                            // (2) start with "remote-shuffle.worker".
                            if (!configKey.startsWith("remote-shuffle.kubernetes.")
                                    && !configKey.startsWith("remote-shuffle.worker.")) {
                                dynamicConfigs.add(
                                        String.format("-D '%s=%s'", kv.getKey(), kv.getValue()));
                            }
                        });
        return String.join(" ", dynamicConfigs);
    }
}
