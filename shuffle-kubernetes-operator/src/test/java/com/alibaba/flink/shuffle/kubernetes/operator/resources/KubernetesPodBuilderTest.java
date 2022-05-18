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
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesPodParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ConfigMapVolume;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.Volume;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesPodBuilder}. */
public class KubernetesPodBuilderTest extends KubernetesTestBase {

    private KubernetesPodParameters podParameters;
    private Pod resultPod;

    @Before
    public void setup() {
        podParameters = new TestingPodParameters();
        resultPod = new KubernetesPodBuilder().buildKubernetesResourceFrom(podParameters);
    }

    @Test
    public void testApiVersion() {
        Assert.assertEquals(Constants.API_VERSION, this.resultPod.getApiVersion());
    }

    @Test
    public void testLabels() {
        assertEquals(USER_LABELS, resultPod.getMetadata().getLabels());
    }

    @Test
    public void testHostNetwork() {
        assertEquals(false, resultPod.getSpec().getHostNetwork());
    }

    @Test
    public void testNodeSelector() {
        assertThat(this.resultPod.getSpec().getNodeSelector(), is(equalTo(NODE_SELECTOR)));
    }

    @Test
    public void testVolumes() {
        List<Volume> volumes = this.resultPod.getSpec().getVolumes();
        assertThat(volumes.size(), is(EMPTY_DIR_VOLUMES.size() + HOST_PATH_VOLUMES.size() + 1));
        List<Volume> emptyDirVolumes = Arrays.asList(volumes.get(0), volumes.get(1));
        List<Volume> hostPathVolumes = Arrays.asList(volumes.get(2), volumes.get(3));
        Volume configMapVolume = volumes.get(4);

        // emptyDir
        List<Map<String, String>> emptyDirParams = new ArrayList<>();
        for (Volume volume : emptyDirVolumes) {
            assertThat(volume.getEmptyDir(), not(nullValue()));

            Map<String, String> emptyDirParam = new HashMap<>();
            emptyDirParam.put("name", volume.getName());
            emptyDirParam.put("sizeLimit", volume.getEmptyDir().getSizeLimit().toString());
            emptyDirParams.add(emptyDirParam);
        }

        assertEquals(
                EMPTY_DIR_VOLUMES.stream()
                        .map(KubernetesUtils::filterEmptyDirVolumeConfigs)
                        .collect(Collectors.toList()),
                emptyDirParams);

        // hostPath
        List<Map<String, String>> hostPathParams = new ArrayList<>();
        for (Volume volume : hostPathVolumes) {
            assertThat(volume.getHostPath(), not(nullValue()));

            Map<String, String> hostPathParam = new HashMap<>();
            hostPathParam.put("name", volume.getName());
            hostPathParam.put("path", volume.getHostPath().getPath());
            hostPathParams.add(hostPathParam);
        }
        assertEquals(
                HOST_PATH_VOLUMES.stream()
                        .map(KubernetesUtils::filterHostPathVolumeConfigs)
                        .collect(Collectors.toList()),
                hostPathParams);

        // configmap
        assertThat(configMapVolume.getConfigMap(), not(nullValue()));
        assertEquals(configMapVolume.getName(), CONFIG_MAP_VOLUME.getVolumeName());
        assertEquals(
                configMapVolume.getConfigMap().getName(), CONFIG_MAP_VOLUME.getConfigMapName());
        assertEquals(
                configMapVolume.getConfigMap().getItems().stream()
                        .collect(Collectors.toMap(item -> item.getKey(), item -> item.getPath())),
                CONFIG_MAP_VOLUME.getItems());
    }

    @Test
    public void testTolerations() {
        List<Map<String, String>> tolerationParams = new ArrayList<>();
        for (Toleration toleration : resultPod.getSpec().getTolerations()) {
            Map<String, String> tolerationParam = new HashMap<>();
            tolerationParam.put("effect", toleration.getEffect());
            tolerationParam.put("key", toleration.getKey());
            tolerationParam.put("operator", toleration.getOperator());
            tolerationParam.put("value", toleration.getValue());

            tolerationParams.add(tolerationParam);
        }
        assertEquals(tolerationParams, TOLERATIONS);
    }

    /** Simple {@link KubernetesPodParameters} implementation for testing purposes. */
    public static class TestingPodParameters implements KubernetesPodParameters {

        @Override
        public Map<String, String> getNodeSelector() {
            return NODE_SELECTOR;
        }

        @Override
        public boolean enablePodHostNetwork() {
            return false;
        }

        @Override
        public List<Map<String, String>> getEmptyDirVolumes() {
            return EMPTY_DIR_VOLUMES;
        }

        @Override
        public List<Map<String, String>> getHostPathVolumes() {
            return HOST_PATH_VOLUMES;
        }

        @Override
        public List<ConfigMapVolume> getConfigMapVolumes() {
            return Collections.singletonList(CONFIG_MAP_VOLUME);
        }

        @Override
        public List<Map<String, String>> getTolerations() {
            return TOLERATIONS;
        }

        @Override
        public KubernetesContainerParameters getContainerParameters() {
            return new KubernetesContainerBuilderTest.TestingContainerParameters();
        }

        @Override
        public String getNamespace() {
            return NAMESPACE;
        }

        @Override
        public Map<String, String> getLabels() {
            return USER_LABELS;
        }
    }
}
