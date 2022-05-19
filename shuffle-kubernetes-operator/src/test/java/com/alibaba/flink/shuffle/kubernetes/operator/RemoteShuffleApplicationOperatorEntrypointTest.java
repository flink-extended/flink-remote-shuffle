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

package com.alibaba.flink.shuffle.kubernetes.operator;

import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplication;

import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceColumnDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Test for {@link RemoteShuffleApplicationOperatorEntrypoint}. */
public class RemoteShuffleApplicationOperatorEntrypointTest {

    @Test
    public void testCreateRemoteShuffleApplicationCRD() {
        CustomResourceDefinition crd =
                RemoteShuffleApplicationOperatorEntrypoint.createRemoteShuffleApplicationCRD();
        RemoteShuffleApplication instance = new RemoteShuffleApplication();

        assertThat(crd.getKind(), is("CustomResourceDefinition"));
        assertThat(crd.getApiVersion(), is("apiextensions.k8s.io/v1beta1"));
        assertThat(crd.getMetadata().getName(), is(instance.getCRDName()));
        assertThat(crd.getSpec().getGroup(), is(instance.getGroup()));
        assertThat(crd.getSpec().getNames().getKind(), is(instance.getKind()));
        assertThat(crd.getSpec().getNames().getSingular(), is(instance.getSingular()));
        assertThat(crd.getSpec().getNames().getPlural(), is(instance.getPlural()));
        assertThat(crd.getSpec().getScope(), is(instance.getScope()));

        assertThat(crd.getSpec().getAdditionalPrinterColumns().size(), is(2));

        List<Triple<String, String, String>> additionalColumns = new ArrayList<>();
        for (CustomResourceColumnDefinition definition :
                crd.getSpec().getAdditionalPrinterColumns()) {
            additionalColumns.add(
                    Triple.of(
                            definition.getName(), definition.getType(), definition.getJSONPath()));
        }

        assertEquals(
                RemoteShuffleApplicationOperatorEntrypoint.ADDITIONAL_COLUMN, additionalColumns);
    }
}
