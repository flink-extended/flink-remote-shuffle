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

package com.alibaba.flink.shuffle.kubernetes.operator.reconciler;

import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplication;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplicationSpec;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import io.fabric8.kubernetes.api.model.apps.DaemonSetBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.fabric8.kubernetes.client.utils.Serialization;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test for {@link RemoteShuffleApplicationReconciler}. */
public class RemoteShuffleApplicationReconcilerTest {

    Map<String, String> dynamicConfigs =
            new HashMap<String, String>() {
                {
                    put(KubernetesOptions.CONTAINER_IMAGE.key(), "flink-remote-shuffle:latest");
                }
            };

    Map<String, String> fileConfigs =
            new HashMap<String, String>() {
                {
                    put("file1", "This is a test for config file.");
                }
            };

    @Test
    public void testReconcile() throws Exception {
        KubernetesServer server =
                new KubernetesServer(
                        false,
                        false,
                        InetAddress.getByName("127.0.0.1"),
                        0,
                        Collections.emptyList());
        server.before();

        String namespace = "default";
        RemoteShuffleApplication testShuffleApp =
                getRemoteShuffleApplication(
                        "testShuffleApp", namespace, "0800cff3-9d80-11ea-8973-0e13a02d8ebd");

        // setup for ConfigMap.
        server.expect()
                .post()
                .withPath("/api/v1/namespaces/" + namespace + "/configmaps")
                .andReturn(
                        HttpURLConnection.HTTP_CREATED,
                        new ConfigMapBuilder().withNewMetadata().endMetadata().build())
                .times(1);

        // setup for ShuffleManager.
        server.expect()
                .post()
                .withPath("/apis/apps/v1/namespaces/" + namespace + "/deployments")
                .andReturn(
                        HttpURLConnection.HTTP_CREATED,
                        new DeploymentBuilder().withNewMetadata().endMetadata().build())
                .times(1);

        // setup for ShuffleWorkers.
        server.expect()
                .post()
                .withPath("/apis/apps/v1/namespaces/" + namespace + "/daemonsets")
                .andReturn(
                        HttpURLConnection.HTTP_CREATED,
                        new DaemonSetBuilder().withNewMetadata().endMetadata().build())
                .times(1);

        KubernetesClient client = server.getClient();
        RemoteShuffleApplicationReconciler reconciler =
                new RemoteShuffleApplicationReconciler(client);

        // trigger reconcile
        reconciler.reconcile(testShuffleApp);

        // check deploy ConfigMap request that server has received.
        RecordedRequest configMapRequest = server.getMockServer().takeRequest();
        assertEquals("POST", configMapRequest.getMethod());

        String configMapRequestBody = configMapRequest.getBody().readUtf8();
        ConfigMap configMapInRequest =
                Serialization.jsonMapper().readValue(configMapRequestBody, ConfigMap.class);
        assertNotNull(configMapInRequest);
        assertEquals(
                configMapInRequest.getMetadata().getName(),
                testShuffleApp.getMetadata().getName() + "-configmap");
        assertEquals(1, configMapInRequest.getMetadata().getOwnerReferences().size());
        assertEquals(
                testShuffleApp.getMetadata().getName(),
                configMapInRequest.getMetadata().getOwnerReferences().get(0).getName());

        // check deploy ShuffleManager request that server has received.
        RecordedRequest shuffleManagerRequest = server.getMockServer().takeRequest();
        assertEquals("POST", shuffleManagerRequest.getMethod());

        String shuffleManagerRequestBody = shuffleManagerRequest.getBody().readUtf8();
        Deployment deploymentInRequest =
                Serialization.jsonMapper().readValue(shuffleManagerRequestBody, Deployment.class);
        assertNotNull(deploymentInRequest);
        Assert.assertEquals(
                KubernetesUtils.getShuffleManagerNameWithClusterId(
                        testShuffleApp.getMetadata().getName()),
                deploymentInRequest.getMetadata().getName());
        assertEquals(1, deploymentInRequest.getMetadata().getOwnerReferences().size());
        assertEquals(
                testShuffleApp.getMetadata().getName(),
                deploymentInRequest.getMetadata().getOwnerReferences().get(0).getName());

        // check deploy ShuffleWorkers request that server has received.
        RecordedRequest shuffleWorkersRequest = server.getMockServer().takeRequest();
        assertEquals("POST", shuffleWorkersRequest.getMethod());

        String shuffleWorkersRequestBody = shuffleWorkersRequest.getBody().readUtf8();
        DaemonSet daemonSetInRequest =
                Serialization.jsonMapper().readValue(shuffleWorkersRequestBody, DaemonSet.class);
        assertNotNull(daemonSetInRequest);
        assertEquals(
                KubernetesUtils.getShuffleWorkersNameWithClusterId(
                        testShuffleApp.getMetadata().getName()),
                daemonSetInRequest.getMetadata().getName());
        assertEquals(1, daemonSetInRequest.getMetadata().getOwnerReferences().size());
        assertEquals(
                testShuffleApp.getMetadata().getName(),
                daemonSetInRequest.getMetadata().getOwnerReferences().get(0).getName());
    }

    private RemoteShuffleApplication getRemoteShuffleApplication(
            String name, String namespace, String uid) {

        RemoteShuffleApplicationSpec shuffleAppSpec = new RemoteShuffleApplicationSpec();
        shuffleAppSpec.setShuffleDynamicConfigs(dynamicConfigs);
        shuffleAppSpec.setShuffleFileConfigs(fileConfigs);

        RemoteShuffleApplication shuffleApp = new RemoteShuffleApplication();
        shuffleApp.getMetadata().setName(name);
        shuffleApp.getMetadata().setNamespace(namespace);
        shuffleApp.getMetadata().setUid(uid);
        shuffleApp.setSpec(shuffleAppSpec);

        return shuffleApp;
    }
}
