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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDeploymentParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesPodParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesDeploymentBuilder}. */
public class KubernetesDeploymentBuilderTest extends KubernetesTestBase {

    private KubernetesDeploymentParameters deploymentParameters;
    private Deployment resultDeployment;

    @Before
    public void setup() {
        deploymentParameters = new TestingDeploymentParameters();
        resultDeployment =
                new KubernetesDeploymentBuilder().buildKubernetesResourceFrom(deploymentParameters);
    }

    @Test
    public void testApiVersion() {
        Assert.assertEquals(Constants.APPS_API_VERSION, resultDeployment.getApiVersion());
    }

    @Test
    public void testDeploymentName() {
        assertEquals(DEPLOYMENT_NAME, resultDeployment.getMetadata().getName());
    }

    @Test
    public void testNameSpace() {
        assertEquals(NAMESPACE, resultDeployment.getMetadata().getNamespace());
    }

    @Test
    public void testLabels() {
        assertEquals(USER_LABELS, resultDeployment.getMetadata().getLabels());
    }

    @Test
    public void testReplicas() {
        assertEquals(Integer.valueOf(1), resultDeployment.getSpec().getReplicas());
    }

    @Test
    public void testSelector() {
        assertEquals(
                USER_LABELS, resultDeployment.getSpec().getTemplate().getMetadata().getLabels());
    }

    /** Simple {@link KubernetesDeploymentParameters} implementation for testing purposes. */
    public static class TestingDeploymentParameters implements KubernetesDeploymentParameters {

        @Override
        public String getDeploymentName() {
            return DEPLOYMENT_NAME;
        }

        @Override
        public int getReplicas() {
            return 1;
        }

        @Override
        public KubernetesPodParameters getPodTemplateParameters() {
            return new KubernetesPodBuilderTest.TestingPodParameters();
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
