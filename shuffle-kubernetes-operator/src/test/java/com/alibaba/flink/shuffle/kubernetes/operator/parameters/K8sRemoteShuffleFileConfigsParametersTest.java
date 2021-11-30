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

import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Test for {@link K8sRemoteShuffleFileConfigsParameters}. */
public class K8sRemoteShuffleFileConfigsParametersTest extends KubernetesTestBase {

    private K8sRemoteShuffleFileConfigsParameters shuffleFileConfigsParameters;

    @Before
    public void setup() {
        shuffleFileConfigsParameters =
                new K8sRemoteShuffleFileConfigsParameters(
                        NAMESPACE, CLUSTER_ID, CONFIG_MAP_VOLUME.getItems());
    }

    @Test
    public void testGetConfigMapName() {
        assertThat(shuffleFileConfigsParameters.getConfigMapName(), is(CLUSTER_ID + "-configmap"));
    }

    @Test
    public void testGetData() {
        Assert.assertEquals(shuffleFileConfigsParameters.getData(), CONFIG_MAP_VOLUME.getItems());
    }

    @Test
    public void testGetNamespace() {
        assertThat(shuffleFileConfigsParameters.getNamespace(), is(NAMESPACE));
    }

    @Test
    public void testGetLabels() {
        assertEquals(shuffleFileConfigsParameters.getLabels(), getCommonLabels());
    }
}
