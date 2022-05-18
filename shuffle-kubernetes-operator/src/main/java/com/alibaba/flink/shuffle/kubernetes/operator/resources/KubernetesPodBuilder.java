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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesPodParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ConfigMapVolume;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.KeyToPath;
import io.fabric8.kubernetes.api.model.KeyToPathBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** Kubernetes Pod builder. */
public class KubernetesPodBuilder
        implements KubernetesResourceBuilder<Pod, KubernetesPodParameters> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesPodBuilder.class);

    @Override
    public Pod buildKubernetesResourceFrom(KubernetesPodParameters podParameters) {

        final Container singleContainer =
                new KubernetesContainerBuilder()
                        .buildKubernetesResourceFrom(podParameters.getContainerParameters());

        PodBuilder podBuilder =
                new PodBuilder()
                        .withApiVersion(Constants.API_VERSION)
                        .editOrNewMetadata()
                        .withLabels(podParameters.getLabels())
                        .endMetadata()
                        .editOrNewSpec()
                        .withHostNetwork(podParameters.enablePodHostNetwork())
                        .withContainers(singleContainer)
                        .withVolumes(getVolumes(podParameters))
                        .withNodeSelector(podParameters.getNodeSelector())
                        .withTolerations(
                                podParameters.getTolerations().stream()
                                        .map(this::getToleration)
                                        .collect(Collectors.toList()))
                        .endSpec();

        return podBuilder.build();
    }

    private List<Volume> getVolumes(KubernetesPodParameters podParameters) {
        List<Volume> volumes = new ArrayList<>();
        // empty dir
        volumes.addAll(
                podParameters.getEmptyDirVolumes().stream()
                        .map(this::getEmptyDirVolume)
                        .collect(Collectors.toList()));
        // host path
        volumes.addAll(
                podParameters.getHostPathVolumes().stream()
                        .map(this::getHostPathVolume)
                        .collect(Collectors.toList()));
        // config map
        for (ConfigMapVolume volume : podParameters.getConfigMapVolumes()) {
            volumes.add(getConfigMapVolume(volume));
        }

        return volumes;
    }

    private Volume getEmptyDirVolume(Map<String, String> stringMap) {
        checkState(stringMap.containsKey(Constants.VOLUME_NAME));

        final VolumeBuilder volumeBuilder = new VolumeBuilder();
        stringMap.forEach(
                (k, v) -> {
                    switch (k) {
                        case Constants.VOLUME_NAME:
                            volumeBuilder.withName(v);
                            break;
                        case Constants.EMPTY_DIR_VOLUME_MEDIUM:
                            volumeBuilder.editOrNewEmptyDir().withMedium(v).endEmptyDir();
                            break;
                        case Constants.EMPTY_DIR_VOLUME_SIZE_LIMIT:
                            volumeBuilder
                                    .editOrNewEmptyDir()
                                    .withSizeLimit(new Quantity(v))
                                    .endEmptyDir();
                            break;
                        default:
                            LOG.warn("Unrecognized key({}) of emptyDir config, will ignore.", k);
                            break;
                    }
                });

        return volumeBuilder.build();
    }

    private Volume getHostPathVolume(Map<String, String> stringMap) {
        checkState(stringMap.containsKey(Constants.VOLUME_NAME));
        checkState(stringMap.containsKey(Constants.HOST_PATH_VOLUME_PATH));

        final VolumeBuilder volumeBuilder = new VolumeBuilder();
        stringMap.forEach(
                (k, v) -> {
                    switch (k) {
                        case Constants.VOLUME_NAME:
                            volumeBuilder.withName(v);
                            break;
                        case Constants.HOST_PATH_VOLUME_TYPE:
                            volumeBuilder.editOrNewHostPath().withType(v).endHostPath();
                            break;
                        case Constants.HOST_PATH_VOLUME_PATH:
                            volumeBuilder.editOrNewHostPath().withPath(v).endHostPath();
                            break;
                        default:
                            LOG.warn("Unrecognized key({}) of hostPath config, will ignore.", k);
                            break;
                    }
                });

        return volumeBuilder.build();
    }

    private Volume getConfigMapVolume(ConfigMapVolume volume) {

        final List<KeyToPath> keyToPaths =
                volume.getItems().entrySet().stream()
                        .map(
                                kv ->
                                        new KeyToPathBuilder()
                                                .withKey(kv.getKey())
                                                .withPath(kv.getValue())
                                                .build())
                        .collect(Collectors.toList());

        return new VolumeBuilder()
                .withName(volume.getVolumeName())
                .editOrNewConfigMap()
                .withName(volume.getConfigMapName())
                .withDefaultMode(420)
                .withItems(keyToPaths)
                .endConfigMap()
                .build();
    }

    private Toleration getToleration(Map<String, String> stringMap) {
        final TolerationBuilder tolerationBuilder = new TolerationBuilder();
        stringMap.forEach(
                (k, v) -> {
                    switch (k.toLowerCase()) {
                        case "effect":
                            tolerationBuilder.withEffect(v);
                            break;
                        case "key":
                            tolerationBuilder.withKey(v);
                            break;
                        case "operator":
                            tolerationBuilder.withOperator(v);
                            break;
                        case "tolerationseconds":
                            tolerationBuilder.withTolerationSeconds(Long.valueOf(v));
                            break;
                        case "value":
                            tolerationBuilder.withValue(v);
                            break;
                        default:
                            LOG.warn("Unrecognized key({}) of toleration, will ignore.", k);
                            break;
                    }
                });
        return tolerationBuilder.build();
    }
}
