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

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesConfigMapParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesConfigMapBuilder}. */
public class KubernetesConfigMapBuilderTest extends KubernetesTestBase {

    private KubernetesConfigMapParameters configMapParameters;
    private ConfigMap resultConfigMap;

    @Before
    public void setup() {
        configMapParameters = new TestingConfigMapParameters();
        resultConfigMap =
                new KubernetesConfigMapBuilder().buildKubernetesResourceFrom(configMapParameters);
    }

    @Test
    public void testApiVersion() {
        Assert.assertEquals(Constants.API_VERSION, resultConfigMap.getApiVersion());
    }

    @Test
    public void testConfigMapName() {
        assertEquals(CONFIG_MAP_NAME, resultConfigMap.getMetadata().getName());
    }

    @Test
    public void testNameSpace() {
        assertEquals(NAMESPACE, resultConfigMap.getMetadata().getNamespace());
    }

    @Test
    public void testLabels() {
        assertEquals(USER_LABELS, resultConfigMap.getMetadata().getLabels());
    }

    /** Simple {@link KubernetesConfigMapParameters} implementation for testing purposes. */
    public static class TestingConfigMapParameters implements KubernetesConfigMapParameters {

        @Override
        public String getConfigMapName() {
            return CONFIG_MAP_NAME;
        }

        @Override
        public Map<String, String> getData() {
            return CONFIG_MAP_VOLUME.getItems();
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
