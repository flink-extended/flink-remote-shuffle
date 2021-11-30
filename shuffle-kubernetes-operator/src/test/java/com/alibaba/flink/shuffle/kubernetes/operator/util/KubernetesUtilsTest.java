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

package com.alibaba.flink.shuffle.kubernetes.operator.util;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesUtils}. */
public class KubernetesUtilsTest {

    @Test
    public void testSetAndGetOwnerReference() {
        Deployment owner =
                new DeploymentBuilder()
                        .editOrNewMetadata()
                        .withName("testOwner")
                        .withNamespace("default")
                        .endMetadata()
                        .build();
        Deployment resource = new DeploymentBuilder().editOrNewMetadata().endMetadata().build();

        KubernetesUtils.setOwnerReference(resource, owner);
        assertEquals(resource.getMetadata().getOwnerReferences().size(), 1);
        OwnerReference ownerReference = KubernetesUtils.getControllerOf(resource);
        assertEquals("testOwner", ownerReference.getName());
        assertEquals("apps/v1", ownerReference.getApiVersion());
        assertEquals(true, ownerReference.getController());
        assertEquals("Deployment", ownerReference.getKind());
        assertEquals(owner.getMetadata().getUid(), ownerReference.getUid());
    }

    @Test
    public void testGetShuffleManagerNameWithClusterId() {
        assertEquals(
                "test-shufflemanager", KubernetesUtils.getShuffleManagerNameWithClusterId("test"));
    }

    @Test
    public void testGetShuffleWorkersNameWithClusterId() {
        assertEquals(
                "test-shuffleworker", KubernetesUtils.getShuffleWorkersNameWithClusterId("test"));
    }

    @Test
    public void testGetResourceFullName() {
        HasMetadata resource =
                new HasMetadata() {
                    @Override
                    public ObjectMeta getMetadata() {
                        return new ObjectMetaBuilder()
                                .withNamespace("testNamespace")
                                .withName("testResource")
                                .build();
                    }

                    @Override
                    public void setMetadata(ObjectMeta objectMeta) {}

                    @Override
                    public void setApiVersion(String s) {}
                };

        assertThat(KubernetesUtils.getResourceFullName(resource), is("testNamespace/testResource"));
    }
}
