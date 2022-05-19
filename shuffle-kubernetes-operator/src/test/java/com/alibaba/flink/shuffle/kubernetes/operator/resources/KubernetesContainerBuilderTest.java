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
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesContainerBuilder}. */
public class KubernetesContainerBuilderTest extends KubernetesTestBase {

    private Container resultContainer;

    @Before
    public void setup() {
        resultContainer =
                new KubernetesContainerBuilder()
                        .buildKubernetesResourceFrom(new TestingContainerParameters());
    }

    @Test
    public void testContainerName() {
        assertEquals(CONTAINER_NAME, resultContainer.getName());
    }

    @Test
    public void testContainerImage() {
        assertEquals(CONTAINER_IMAGE, resultContainer.getImage());
        assertEquals("Always", resultContainer.getImagePullPolicy());
    }

    @Test
    public void testMainContainerResourceRequirements() {
        final ResourceRequirements resourceRequirements = resultContainer.getResources();

        final Map<String, Quantity> requests = resourceRequirements.getRequests();
        final Map<String, Quantity> limits = resourceRequirements.getLimits();

        assertEquals(Double.toString(CONTAINER_CPU), requests.get("cpu").getAmount());
        assertEquals(
                Double.toString(
                        CONTAINER_CPU * Double.parseDouble(RESOURCE_LIMIT_FACTOR.get("cpu"))),
                limits.get("cpu").getAmount());
        assertEquals(String.valueOf(getTotalMemory(false)), requests.get("memory").getAmount());
        assertEquals(
                String.valueOf(
                        getTotalMemory(false)
                                * Double.parseDouble(RESOURCE_LIMIT_FACTOR.get("memory"))),
                limits.get("memory").getAmount());
    }

    @Test
    public void testContainerCommandAndArgs() {
        assertEquals(Collections.singletonList("exec"), resultContainer.getCommand());
        assertEquals(Arrays.asList("bash", "-c", "sleep"), resultContainer.getArgs());
    }

    @Test
    public void testContainerVolumeMounts() {
        List<Map<String, String>> resultVolumeMounts = new ArrayList<>();
        for (VolumeMount volumeMount : resultContainer.getVolumeMounts()) {
            Map<String, String> resultVolumeMount = new HashMap<>();
            resultVolumeMount.put("name", volumeMount.getName());
            resultVolumeMount.put("mountPath", volumeMount.getMountPath());
            resultVolumeMounts.add(resultVolumeMount);
        }

        assertEquals(CONTAINER_VOLUME_MOUNTS, resultVolumeMounts);
    }

    /** Simple {@link KubernetesContainerParameters} implementation for testing purposes. */
    public static class TestingContainerParameters implements KubernetesContainerParameters {

        @Override
        public String getContainerName() {
            return CONTAINER_NAME;
        }

        @Override
        public String getContainerImage() {
            return CONTAINER_IMAGE;
        }

        @Override
        public String getContainerImagePullPolicy() {
            return "Always";
        }

        @Override
        public List<Map<String, String>> getContainerVolumeMounts() {
            return CONTAINER_VOLUME_MOUNTS;
        }

        @Override
        public Integer getContainerMemoryMB() {
            return getTotalMemory(false);
        }

        @Override
        public Double getContainerCPU() {
            return CONTAINER_CPU;
        }

        @Override
        public Map<String, String> getResourceLimitFactors() {
            return RESOURCE_LIMIT_FACTOR;
        }

        @Override
        public ContainerCommandAndArgs getContainerCommandAndArgs() {
            return new ContainerCommandAndArgs("exec", Arrays.asList("bash", "-c", "sleep"));
        }

        @Override
        public Map<String, String> getEnvironmentVars() {
            return Collections.emptyMap();
        }
    }
}
