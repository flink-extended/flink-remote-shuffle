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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.functions.RunnableWithException;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplication;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.K8sRemoteShuffleFileConfigsParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesConfigMapParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDaemonSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesShuffleManagerParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesShuffleWorkerParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesStatefulSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesConfigMapBuilder;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesDaemonSetBuilder;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesDeploymentBuilder;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesStatefulSetBuilder;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesInternalOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * The {@link RemoteShuffleApplicationReconciler} is responsible for reconciling {@link
 * RemoteShuffleApplication} to the desired state which is described by {@code
 * RemoteShuffleApplication#spec}.
 */
public class RemoteShuffleApplicationReconciler {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoteShuffleApplicationReconciler.class);

    private final KubernetesClient kubeClient;

    public RemoteShuffleApplicationReconciler(KubernetesClient kubeClient) {
        this.kubeClient = kubeClient;
    }

    /**
     * This method will trigger the reconciliation of RemoteShuffleApplication to the desired state.
     *
     * @param shuffleApp the RemoteShuffleApplication.
     */
    public void reconcile(RemoteShuffleApplication shuffleApp) {
        final String namespace = shuffleApp.getMetadata().getNamespace();
        final String clusterId = shuffleApp.getMetadata().getName();

        LOG.info("Reconciling RemoteShuffleApplication {}/{}", namespace, clusterId);

        final Configuration dynamicConfigs =
                Configuration.fromMap(shuffleApp.getSpec().getShuffleDynamicConfigs());
        dynamicConfigs.setString(KubernetesInternalOptions.NAMESPACE, namespace);
        dynamicConfigs.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, clusterId);

        final Map<String, String> fileConfigs = shuffleApp.getSpec().getShuffleFileConfigs();

        Configuration updatedDynamicConfigs = dynamicConfigs;
        if (fileConfigs != null && !fileConfigs.isEmpty()) {
            updatedDynamicConfigs = reconcileFileConfigs(dynamicConfigs, fileConfigs, shuffleApp);
        }
        reconcileShuffleManager(updatedDynamicConfigs, shuffleApp);
        reconcileShuffleWorkers(updatedDynamicConfigs, shuffleApp);
    }

    /**
     * This method will trigger the deployment of ShuffleManager. ShuffleManager will be deployed in
     * the form of a Kubernetes {@link Deployment}.
     *
     * @param configuration configuration of the ShuffleManager
     * @param owner The owner of ShuffleManager({@link Deployment}). The owner is set for garbage
     *     collection. When the owner is deleted, the ShuffleManager will be deleted automatically.
     */
    private void reconcileShuffleManager(Configuration configuration, HasMetadata owner) {
        KubernetesShuffleManagerParameters shuffleManagerParameters =
                new KubernetesShuffleManagerParameters(configuration);
        Deployment deployment =
                new KubernetesDeploymentBuilder()
                        .buildKubernetesResourceFrom(shuffleManagerParameters);
        KubernetesUtils.setOwnerReference(deployment, owner);

        LOG.info("Reconcile shuffle manager {}.", deployment.getMetadata().getName());
        executeReconcileWithRetry(
                () -> {
                    LOG.debug("Try to create or update Deployment {}.", deployment.toString());
                    this.kubeClient
                            .apps()
                            .deployments()
                            .inNamespace(
                                    checkNotNull(
                                            configuration.getString(
                                                    KubernetesInternalOptions.NAMESPACE)))
                            .createOrReplace(deployment);
                },
                deployment.getMetadata().getName());
    }

    /**
     * This method will trigger the deployment of ShuffleWorkers. ShuffleWorkers will be deployed in
     * the form of a Kubernetes {@link DaemonSet}.
     *
     * @param configuration configuration of the ShuffleWorkers
     * @param owner The owner of ShuffleWorkers({@link DaemonSet}). The owner is set for garbage
     *     collection. When the owner is deleted, the ShuffleWorkers will be deleted automatically.
     */
    private void reconcileShuffleWorkers(Configuration configuration, HasMetadata owner) {

        KubernetesOptions.WorkerMode mode =
                KubernetesOptions.WorkerMode.fromString(
                        configuration.getString(KubernetesOptions.SHUFFLE_WORKER_DEPLOY_MODE));

        switch (mode) {
            case STATEFUL_SETS:
                KubernetesStatefulSetParameters statefulSetParameters =
                        new KubernetesShuffleWorkerParameters(configuration);
                StatefulSet statefulSet =
                        KubernetesStatefulSetBuilder.INSTANCE.buildKubernetesResourceFrom(
                                statefulSetParameters);
                KubernetesUtils.setOwnerReference(statefulSet, owner);
                LOG.info("Reconcile shuffle workers {}.", statefulSet.getMetadata().getName());
                executeReconcileWithRetry(
                        () -> {
                            LOG.debug("Try to create or update StatefulSet {}.", statefulSet);
                            this.kubeClient
                                    .apps()
                                    .statefulSets()
                                    .inNamespace(
                                            checkNotNull(
                                                    configuration.getString(
                                                            KubernetesInternalOptions.NAMESPACE)))
                                    .createOrReplace(statefulSet);
                        },
                        statefulSet.getMetadata().getName());
                break;
            case DAEMON_SETS:
                KubernetesDaemonSetParameters shuffleWorkerParameters =
                        new KubernetesShuffleWorkerParameters(configuration);
                DaemonSet daemonSet =
                        new KubernetesDaemonSetBuilder()
                                .buildKubernetesResourceFrom(shuffleWorkerParameters);
                KubernetesUtils.setOwnerReference(daemonSet, owner);
                LOG.info("Reconcile shuffle workers {}.", daemonSet.getMetadata().getName());
                executeReconcileWithRetry(
                        () -> {
                            LOG.debug(
                                    "Try to create or update DaemonSet {}.", daemonSet.toString());
                            this.kubeClient
                                    .apps()
                                    .daemonSets()
                                    .inNamespace(
                                            checkNotNull(
                                                    configuration.getString(
                                                            KubernetesInternalOptions.NAMESPACE)))
                                    .createOrReplace(daemonSet);
                        },
                        daemonSet.getMetadata().getName());
        }
    }

    /**
     * This method will trigger the deployment of shuffle config files. The shuffle config files
     * will be deployed in the form of a Kubernetes {@link ConfigMap}. And then the config map will
     * be mounted into ShuffleManager and ShuffleWorkers pods.
     *
     * @param fileConfigs Config file contents.
     * @param owner The owner of ShuffleManager({@link Deployment}). The owner is set for garbage
     *     collection. When the owner is deleted, the {@link ConfigMap} will be deleted
     *     automatically.
     * @return the updated configuration.
     */
    private Configuration reconcileFileConfigs(
            Configuration dynamicParameters, Map<String, String> fileConfigs, HasMetadata owner) {

        // TODO: Currently, pods will not restart automatically when the content of the file is
        // changed. Will support later.

        KubernetesConfigMapParameters configMapParameters =
                new K8sRemoteShuffleFileConfigsParameters(
                        dynamicParameters.getString(KubernetesInternalOptions.NAMESPACE),
                        dynamicParameters.getString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID),
                        fileConfigs);

        ConfigMap configMap =
                new KubernetesConfigMapBuilder().buildKubernetesResourceFrom(configMapParameters);

        String volumeName = Constants.REMOTE_SHUFFLE_CONF_VOLUME;
        String configMapName = configMap.getMetadata().getName();

        Configuration configuration = dynamicParameters;

        // setup configmap
        configuration =
                setupConfigMapVolume(
                        configuration, volumeName, configMapName, fileConfigs.keySet());

        // set owner
        KubernetesUtils.setOwnerReference(configMap, owner);

        final Configuration finalConfiguration = configuration;
        // deploy configmap
        LOG.info("Reconcile configmap {}.", configMap.getMetadata().getName());
        executeReconcileWithRetry(
                () -> {
                    LOG.debug("Try to create or update ConfigMap {}.", configMap.toString());
                    this.kubeClient
                            .configMaps()
                            .inNamespace(
                                    finalConfiguration.getString(
                                            KubernetesInternalOptions.NAMESPACE))
                            .withName(configMapName)
                            .createOrReplace(configMap);
                },
                configMap.getMetadata().getName());

        return configuration;
    }

    private void executeReconcileWithRetry(RunnableWithException action, String component) {
        KubernetesUtils.executeWithRetry(action, String.format("Reconcile %s", component));
    }

    private Configuration setupConfigMapVolume(
            Configuration config,
            String volumeName,
            String configMapName,
            Collection<String> fileNames) {

        Configuration configuration = new Configuration(config);

        // set volume
        configuration.setString(KubernetesInternalOptions.CONFIG_VOLUME_NAME, volumeName);
        configuration.setString(
                KubernetesInternalOptions.CONFIG_VOLUME_CONFIG_MAP_NAME, configMapName);
        configuration.setMap(
                KubernetesInternalOptions.CONFIG_VOLUME_ITEMS,
                fileNames.stream().collect(Collectors.toMap(file -> file, file -> file)));
        configuration.setString(
                KubernetesInternalOptions.CONFIG_VOLUME_MOUNT_PATH,
                Constants.REMOTE_SHUFFLE_CONF_DIR);

        return configuration;
    }
}
