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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesInternalOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test for {@link AbstractKubernetesParameters}. */
public class AbstractKubernetesParametersTest extends KubernetesTestBase {

    private final Configuration conf = new Configuration();
    private final TestingKubernetesParameters testingKubernetesParameters =
            new TestingKubernetesParameters(conf);

    @Test
    public void testGetNamespace() {
        conf.setString(KubernetesInternalOptions.NAMESPACE, NAMESPACE);
        assertThat(testingKubernetesParameters.getNamespace(), is(NAMESPACE));
    }

    @Test
    public void testEnablePodHostNetwork() {
        conf.setBoolean(KubernetesOptions.POD_HOST_NETWORK_ENABLED, true);
        assertThat(testingKubernetesParameters.enablePodHostNetwork(), is(true));
    }

    @Test
    public void testGetContainerImage() {
        conf.setString(KubernetesOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
        assertThat(testingKubernetesParameters.getContainerImage(), is(CONTAINER_IMAGE));
    }

    @Test
    public void testGetContainerImagePullPolicy() {
        conf.setString(KubernetesOptions.CONTAINER_IMAGE_PULL_POLICY, "Always");

        assertThat(testingKubernetesParameters.getContainerImagePullPolicy(), is("Always"));
    }

    @Test
    public void testGetClusterId() {
        conf.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, CLUSTER_ID);
        assertThat(testingKubernetesParameters.getClusterId(), is(CLUSTER_ID));
    }

    @Test
    public void testClusterIdMustNotBeBlank() {
        conf.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, "  ");
        assertThrows(
                "must not be blank",
                IllegalArgumentException.class,
                testingKubernetesParameters::getClusterId);
    }

    @Test
    public void testClusterIdLengthLimitation() {
        final String stringWithIllegalLength =
                CommonUtils.randomHexString(Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + 1);
        conf.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, stringWithIllegalLength);
        assertThrows(
                "must be no more than "
                        + Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID
                        + " characters",
                IllegalArgumentException.class,
                testingKubernetesParameters::getClusterId);
    }

    @Test
    public void testInvalidContainerImage() {
        conf.setString(KubernetesOptions.CONTAINER_IMAGE, "  ");
        assertThrows(
                "Invalid " + KubernetesOptions.CONTAINER_IMAGE + ".",
                IllegalArgumentException.class,
                testingKubernetesParameters::getContainerImage);
    }

    /** Checks whether an exception with a message occurs when running a piece of code. */
    public static void assertThrows(
            String msg, Class<? extends Exception> expected, Callable<?> code) {
        try {
            Object result = code.call();
            Assert.fail("Previous method call should have failed but it returned: " + result);
        } catch (Exception e) {
            assertThat(e, instanceOf(expected));
            assertThat(e.getMessage(), containsString(msg));
        }
    }

    /** Simple subclass of {@link AbstractKubernetesParameters} for testing purposes. */
    public static class TestingKubernetesParameters extends AbstractKubernetesParameters {

        public TestingKubernetesParameters(Configuration conf) {
            super(conf);
        }

        @Override
        public Map<String, String> getLabels() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getNodeSelector() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public List<Map<String, String>> getEmptyDirVolumes() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public List<Map<String, String>> getHostPathVolumes() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public List<Map<String, String>> getTolerations() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public String getContainerName() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public List<Map<String, String>> getContainerVolumeMounts() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Integer getContainerMemoryMB() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Double getContainerCPU() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getResourceLimitFactors() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public ContainerCommandAndArgs getContainerCommandAndArgs() {
            throw new UnsupportedOperationException("NOT supported");
        }

        @Override
        public Map<String, String> getEnvironmentVars() {
            throw new UnsupportedOperationException("NOT supported");
        }
    }
}
