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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesContainerParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesPodParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesStatefulSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesStatefulSetBuilder}. */
public class KubernetesStatefulSetBuilderTest extends KubernetesTestBase {

    private StatefulSet statefulSet;

    @Before
    public void setup() throws JsonProcessingException {
        KubernetesStatefulSetParameters statefulSetParameters = new TestingStatefulSetsParameters();
        statefulSet =
                KubernetesStatefulSetBuilder.INSTANCE.buildKubernetesResourceFrom(
                        statefulSetParameters);
    }

    @Test
    public void testStatefulSetName() {
        assertEquals(STATEFUL_SET_NAME, statefulSet.getMetadata().getName());
    }

    @Test
    public void testNameSpace() {
        assertEquals(NAMESPACE, statefulSet.getMetadata().getNamespace());
    }

    @Test
    public void testLabels() {
        assertEquals(USER_LABELS, statefulSet.getMetadata().getLabels());
    }

    @Test
    public void testReplicas() {
        assertEquals(Integer.valueOf(WORKER_REPLICAS), statefulSet.getSpec().getReplicas());
    }

    @Test
    public void testSelector() {
        assertEquals(USER_LABELS, statefulSet.getSpec().getTemplate().getMetadata().getLabels());
    }

    @Test
    public void testPvc() {
        Assert.assertEquals(1, statefulSet.getSpec().getVolumeClaimTemplates().size());
        PersistentVolumeClaim pvc = statefulSet.getSpec().getVolumeClaimTemplates().get(0);
        assertEquals(STORAGE_RESOURCES, pvc.getSpec().getResources().getRequests());
        assertEquals(STORAGE_RESOURCES, pvc.getSpec().getResources().getLimits());
        assertEquals(STORAGE_CLASS, pvc.getSpec().getStorageClassName());
        assertEquals(ACCESS_MODE, pvc.getSpec().getAccessModes());

        // pvc mounted
        List<Container> containers =
                statefulSet.getSpec().getTemplate().getSpec().getContainers().stream()
                        .filter(
                                c ->
                                        c.getName()
                                                .equals(
                                                        KubernetesUtils
                                                                .SHUFFLE_WORKER_CONTAINER_NAME))
                        .collect(Collectors.toList());
        Assert.assertEquals(1, containers.size());
        boolean exists = false;
        for (VolumeMount volumeMount : containers.get(0).getVolumeMounts()) {
            if (volumeMount.getName().equals(pvc.getMetadata().getName())) {
                exists = true;
                Assert.assertEquals(MOUNT_PATH, volumeMount.getMountPath());
                break;
            }
        }
        Assert.assertTrue("The pvc not mounted to the pod template", exists);
    }

    /** Simple {@link KubernetesStatefulSetParameters} implementation for testing purposes. */
    public static class TestingStatefulSetsParameters implements KubernetesStatefulSetParameters {

        @Override
        public String getNamespace() {
            return NAMESPACE;
        }

        @Override
        public Map<String, String> getLabels() {
            return USER_LABELS;
        }

        @Override
        public String getStatefulSetName() {
            return STATEFUL_SET_NAME;
        }

        @Override
        public KubernetesPodParameters getPodTemplateParameters() {
            return new TestingPodParametersWithContainerName();
        }

        @Override
        public int getReplicas() {
            return WORKER_REPLICAS;
        }

        @Override
        public Map<String, Quantity> getStorageResource() {
            return STORAGE_RESOURCES;
        }

        @Override
        public String getStorageClass() {
            return STORAGE_CLASS;
        }

        @Override
        public List<String> getAccessMode() {
            return ACCESS_MODE;
        }

        @Override
        public String getMountPath() {
            return MOUNT_PATH;
        }
    }

    /** Simple {@link KubernetesPodParameters} implementation for testing purposes. */
    public static class TestingPodParametersWithContainerName
            extends KubernetesPodBuilderTest.TestingPodParameters {
        @Override
        public KubernetesContainerParameters getContainerParameters() {
            return new TestingContainerParametersWithContainerName();
        }
    }

    /** Simple {@link KubernetesContainerParameters} implementation for testing purposes. */
    public static class TestingContainerParametersWithContainerName
            extends KubernetesContainerBuilderTest.TestingContainerParameters {
        @Override
        public String getContainerName() {
            return KubernetesUtils.SHUFFLE_WORKER_CONTAINER_NAME;
        }
    }
}
