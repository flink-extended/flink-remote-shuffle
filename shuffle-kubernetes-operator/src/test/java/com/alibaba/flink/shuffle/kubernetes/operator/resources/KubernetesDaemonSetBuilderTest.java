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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDaemonSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesPodParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesDaemonSetBuilder}. */
public class KubernetesDaemonSetBuilderTest extends KubernetesTestBase {

    private KubernetesDaemonSetParameters daemonSetParameters;
    private DaemonSet resultDaemonSet;

    @Before
    public void setup() {
        daemonSetParameters = new TestingDaemonSetParameters();
        resultDaemonSet =
                new KubernetesDaemonSetBuilder().buildKubernetesResourceFrom(daemonSetParameters);
    }

    @Test
    public void testApiVersion() {
        Assert.assertEquals(Constants.APPS_API_VERSION, resultDaemonSet.getApiVersion());
    }

    @Test
    public void testDaemonSetName() {
        assertThat(DAEMON_SET_NAME, is(resultDaemonSet.getMetadata().getName()));
    }

    @Test
    public void testNameSpace() {
        assertEquals(NAMESPACE, resultDaemonSet.getMetadata().getNamespace());
    }

    @Test
    public void testLabels() {
        assertEquals(USER_LABELS, resultDaemonSet.getMetadata().getLabels());
    }

    /** Simple {@link KubernetesDaemonSetParameters} implementation for testing purposes. */
    public static class TestingDaemonSetParameters implements KubernetesDaemonSetParameters {

        @Override
        public String getDaemonSetName() {
            return DAEMON_SET_NAME;
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
