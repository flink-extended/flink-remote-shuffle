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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.core.config.memory.ShuffleWorkerProcessSpec;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ConfigMapVolume;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.apps.DaemonSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * ShuffleWorkers will be deployed as a {@link DaemonSet} in Kubernetes. This class helps to parse
 * ShuffleWorkers configuration to kubernetes {@link DaemonSet} configuration.
 */
public class KubernetesShuffleWorkerParameters extends AbstractKubernetesParameters
        implements KubernetesDaemonSetParameters {

    private final ShuffleWorkerProcessSpec shuffleWorkerProcessSpec;

    public KubernetesShuffleWorkerParameters(Configuration conf) {
        super(conf);
        this.shuffleWorkerProcessSpec = new ShuffleWorkerProcessSpec(conf);
    }

    @Override
    public Map<String, String> getLabels() {
        final Map<String, String> labels = new HashMap<>();
        labels.putAll(
                Optional.ofNullable(conf.getMap(KubernetesOptions.SHUFFLE_WORKER_LABELS))
                        .orElse(Collections.emptyMap()));
        labels.putAll(KubernetesUtils.getCommonLabels(getClusterId()));
        labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_SHUFFLE_WORKER);
        return Collections.unmodifiableMap(labels);
    }

    @Override
    public Map<String, String> getNodeSelector() {
        return Optional.ofNullable(conf.getMap(KubernetesOptions.SHUFFLE_WORKER_NODE_SELECTOR))
                .orElse(Collections.emptyMap());
    }

    @Override
    public String getContainerName() {
        return KubernetesUtils.SHUFFLE_WORKER_CONTAINER_NAME;
    }

    @Override
    public Integer getContainerMemoryMB() {
        return shuffleWorkerProcessSpec.getTotalProcessMemorySize().getMebiBytes();
    }

    @Override
    public Double getContainerCPU() {
        return conf.getDouble(KubernetesOptions.SHUFFLE_WORKER_CPU);
    }

    @Override
    public Map<String, String> getResourceLimitFactors() {
        return KubernetesUtils.getPrefixedKeyValuePairs(
                KubernetesOptions.SHUFFLE_WORKER_RESOURCE_LIMIT_FACTOR_PREFIX, conf);
    }

    @Override
    public ContainerCommandAndArgs getContainerCommandAndArgs() {
        String command = "bash";
        return new ContainerCommandAndArgs(
                command,
                Arrays.asList(
                        "-c",
                        Constants.SHUFFLE_WORKER_SCRIPT_PATH + " " + getShuffleWorkerConfigs()));
    }

    @Override
    public List<Map<String, String>> getContainerVolumeMounts() {
        List<Map<String, String>> volumeMountsConfigs = new ArrayList<>();

        // empty dir volume mounts
        volumeMountsConfigs.addAll(
                KubernetesUtils.filterVolumesConfigs(
                        conf,
                        KubernetesOptions.SHUFFLE_WORKER_EMPTY_DIR_VOLUMES,
                        KubernetesUtils::filterVolumeMountsConfigs));

        // host path volume mounts
        volumeMountsConfigs.addAll(
                KubernetesUtils.filterVolumesConfigs(
                        conf,
                        KubernetesOptions.SHUFFLE_WORKER_HOST_PATH_VOLUMES,
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
    public List<Map<String, String>> getEmptyDirVolumes() {
        return KubernetesUtils.filterVolumesConfigs(
                conf,
                KubernetesOptions.SHUFFLE_WORKER_EMPTY_DIR_VOLUMES,
                KubernetesUtils::filterEmptyDirVolumeConfigs);
    }

    @Override
    public List<Map<String, String>> getHostPathVolumes() {
        return KubernetesUtils.filterVolumesConfigs(
                conf,
                KubernetesOptions.SHUFFLE_WORKER_HOST_PATH_VOLUMES,
                KubernetesUtils::filterHostPathVolumeConfigs);
    }

    @Override
    public List<Map<String, String>> getTolerations() {
        return Optional.ofNullable(
                        conf.getList(KubernetesOptions.SHUFFLE_WORKER_TOLERATIONS, Map.class))
                .orElse(Collections.emptyList());
    }

    @Override
    public Map<String, String> getEnvironmentVars() {
        Map<String, String> envVars = new HashMap<>();
        envVars.put(Constants.LABEL_APPTYPE_KEY.toUpperCase(), Constants.LABEL_APPTYPE_VALUE);
        envVars.put(Constants.LABEL_APP_KEY.toUpperCase(), getClusterId());
        envVars.put(
                Constants.LABEL_COMPONENT_KEY.toUpperCase(),
                Constants.LABEL_COMPONENT_SHUFFLE_WORKER);
        envVars.putAll(conf.getMap(KubernetesOptions.SHUFFLE_WORKER_ENV_VARS));
        return envVars;
    }

    private String getShuffleWorkerConfigs() {
        List<String> dynamicConfigs = new ArrayList<>();
        conf.toMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEachOrdered(
                        kv -> {
                            String configKey = kv.getKey();
                            // following configs should not be passed to ShuffleWorker:
                            // (1) start with "remote-shuffle.kubernetes".
                            // (2) start with "remote-shuffle.manager".
                            if (!configKey.startsWith("remote-shuffle.kubernetes.")
                                    && !configKey.startsWith("remote-shuffle.manager.")) {
                                dynamicConfigs.add(
                                        String.format("-D '%s=%s'", kv.getKey(), kv.getValue()));
                            }
                        });
        return String.join(" ", dynamicConfigs);
    }

    @Override
    public String getDaemonSetName() {
        return KubernetesUtils.getShuffleWorkersNameWithClusterId(getClusterId());
    }

    @Override
    public KubernetesPodParameters getPodTemplateParameters() {
        return this;
    }
}
