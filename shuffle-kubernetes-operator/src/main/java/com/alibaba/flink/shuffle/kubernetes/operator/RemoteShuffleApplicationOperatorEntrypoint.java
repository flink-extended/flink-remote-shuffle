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

import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.kubernetes.operator.controller.RemoteShuffleApplicationController;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplication;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;

import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apiextensions.v1beta1.CustomResourceDefinitionBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.base.CustomResourceDefinitionContext;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** Entrypoint class of {@link RemoteShuffleApplication} Operator. */
public class RemoteShuffleApplicationOperatorEntrypoint {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoteShuffleApplicationOperatorEntrypoint.class);

    static final List<Triple<String, String, String>> ADDITIONAL_COLUMN =
            new ArrayList<Triple<String, String, String>>() {
                {
                    add(
                            Triple.of(
                                    "SHUFFLE_MANAGER_READY",
                                    "string",
                                    ".status.readyShuffleManagers"));
                    add(Triple.of("SHUFFLE_WORKER_READY", "string", ".status.readyShuffleWorkers"));
                }
            };

    public static void main(String[] args) {
        Config config = Config.autoConfigure(null);
        String namespace = config.getNamespace();
        if (namespace == null) {
            LOG.info("No namespace found via config, assuming default.");
            config.setNamespace("default");
        }
        LOG.info("Using namespace {}.", config.getNamespace());

        config.setUserAgent(Constants.REMOTE_SHUFFLE_OPERATOR_USER_AGENT);
        LOG.info("Setting user agent for Kubernetes client to {}", config.getUserAgent());

        final KubernetesClient kubeClient = new DefaultKubernetesClient(config);
        final ExecutorService executorPool =
                Executors.newFixedThreadPool(10, new ExecutorThreadFactory("informers"));
        final SharedInformerFactory informerFactory =
                kubeClient.informers(executorPool).inNamespace(kubeClient.getNamespace());
        try {

            CustomResourceDefinition shuffleApplicationCRD = createRemoteShuffleApplicationCRD();
            // create CRD for the flink remote shuffle service.
            kubeClient
                    .apiextensions()
                    .v1beta1()
                    .customResourceDefinitions()
                    .createOrReplace(shuffleApplicationCRD);

            RemoteShuffleApplicationController remoteShuffleApplicationController =
                    RemoteShuffleApplicationController.createRemoteShuffleApplicationController(
                            kubeClient, informerFactory);

            informerFactory.startAllRegisteredInformers();
            informerFactory.addSharedInformerEventListener(
                    exception -> LOG.error("Exception occurred, but caught", exception));
            remoteShuffleApplicationController.run();
        } catch (Throwable throwable) {
            LOG.error("Remote shuffle application operator terminated.", throwable);
        } finally {
            informerFactory.stopAllRegisteredInformers();
            kubeClient.close();
        }
    }

    static CustomResourceDefinition createRemoteShuffleApplicationCRD() {
        // create CRD builder from context.
        CustomResourceDefinitionBuilder customCRDBuilder =
                CustomResourceDefinitionContext.v1beta1CRDFromCustomResourceType(
                        RemoteShuffleApplication.class);

        // setup additional print columns.
        ADDITIONAL_COLUMN.forEach(
                column -> {
                    customCRDBuilder
                            .editSpec()
                            .addNewAdditionalPrinterColumn()
                            .withName(column.getLeft())
                            .withType(column.getMiddle())
                            .withJSONPath(column.getRight())
                            .endAdditionalPrinterColumn()
                            .endSpec();
                });

        // setup status
        customCRDBuilder
                .editOrNewSpec()
                .editOrNewSubresources()
                .withNewStatus()
                .endStatus()
                .endSubresources()
                .endSpec();

        return customCRDBuilder.build();
    }
}
