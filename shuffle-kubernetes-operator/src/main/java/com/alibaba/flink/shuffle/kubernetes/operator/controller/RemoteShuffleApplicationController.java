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

package com.alibaba.flink.shuffle.kubernetes.operator.controller;

import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplication;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplicationList;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplicationSpec;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplicationStatus;
import com.alibaba.flink.shuffle.kubernetes.operator.reconciler.RemoteShuffleApplicationReconciler;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import io.fabric8.kubernetes.api.model.apps.DaemonSetStatus;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.fabric8.kubernetes.client.informers.cache.Lister;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Controller of {@link RemoteShuffleApplication}. This class is responsible for following things:
 *
 * <p>1. Monitor the addition, modification and deletion requests of {@link
 * RemoteShuffleApplication}, and take actions.
 *
 * <p>2. Monitor the status of {@link RemoteShuffleApplication} internal components (ShuffleManager
 * and ShuffleWorkers). Once they are not in the desired state, reconciling them by {@link
 * RemoteShuffleApplicationReconciler}.
 */
public class RemoteShuffleApplicationController {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoteShuffleApplicationController.class);
    private static final long RSYNC_MILLIS = 10 * 60 * 1000;
    private static final long UPDATE_STATUS_INTERVAL_MS = 1000L;

    private final BlockingQueue<Pair<String, RemoteShuffleAction>> workQueue;
    private final SharedIndexInformer<RemoteShuffleApplication> shuffleAppInformer;
    private final SharedIndexInformer<Deployment> deploymentInformer;
    private final SharedIndexInformer<DaemonSet> daemonSetInformer;

    private final Lister<Deployment> deploymentLister;
    private final Lister<DaemonSet> daemonSetLister;
    private final Lister<RemoteShuffleApplication> shuffleAppLister;

    private final MixedOperation<
                    RemoteShuffleApplication,
                    RemoteShuffleApplicationList,
                    Resource<RemoteShuffleApplication>>
            shuffleAppClient;

    private final RemoteShuffleApplicationReconciler shuffleAppReconciler;
    private volatile long lastUpdateStatusTime = 0L;

    AtomicBoolean isRunning = new AtomicBoolean(false);

    private enum ComponentEventType {
        ADD,
        DELETE,
        UPDATE_SPEC,
        UPDATE_STATUS
    }

    private enum RemoteShuffleAction {
        ADD_OR_UPDATE,
        UPDATE_STATUS
    }

    public RemoteShuffleApplicationController(
            SharedIndexInformer<RemoteShuffleApplication> shuffleAppInformer,
            SharedIndexInformer<Deployment> deploymentInformer,
            SharedIndexInformer<DaemonSet> daemonSetInformer,
            KubernetesClient kubeClient,
            MixedOperation<
                            RemoteShuffleApplication,
                            RemoteShuffleApplicationList,
                            Resource<RemoteShuffleApplication>>
                    shuffleAppClient) {

        this.shuffleAppInformer = shuffleAppInformer;
        this.deploymentInformer = deploymentInformer;
        this.daemonSetInformer = daemonSetInformer;
        this.shuffleAppClient = shuffleAppClient;

        this.shuffleAppReconciler = new RemoteShuffleApplicationReconciler(kubeClient);
        this.shuffleAppLister = new Lister<>(shuffleAppInformer.getIndexer());
        this.deploymentLister = new Lister<>(deploymentInformer.getIndexer());
        this.daemonSetLister = new Lister<>(daemonSetInformer.getIndexer());

        // TODO: Removing the duplicate key.
        this.workQueue = new ArrayBlockingQueue<>(4096);
    }

    public void create() {
        // Monitor the addition, modification and deletion requests.
        shuffleAppInformer.addEventHandler(
                new ResourceEventHandler<RemoteShuffleApplication>() {
                    @Override
                    public void onAdd(RemoteShuffleApplication shuffleApp) {
                        LOG.info(
                                "RemoteShuffleApplication {} add.",
                                KubernetesUtils.getResourceFullName(shuffleApp));
                        enqueueRemoteShuffleApplication(
                                shuffleApp, RemoteShuffleAction.ADD_OR_UPDATE);
                    }

                    @Override
                    public void onUpdate(
                            RemoteShuffleApplication oldShuffleApp,
                            RemoteShuffleApplication newShuffleApp) {
                        // Only spec changed, we do reconcile.
                        if (!Objects.equals(oldShuffleApp.getSpec(), newShuffleApp.getSpec())) {
                            LOG.info(
                                    "RemoteShuffleApplication {} spec update.",
                                    KubernetesUtils.getResourceFullName(newShuffleApp));
                            enqueueRemoteShuffleApplication(
                                    newShuffleApp, RemoteShuffleAction.ADD_OR_UPDATE);
                        }
                    }

                    @Override
                    public void onDelete(RemoteShuffleApplication shuffleApp, boolean b) {
                        LOG.info(
                                "RemoteShuffleApplication {} delete.",
                                KubernetesUtils.getResourceFullName(shuffleApp));
                        // do nothing.
                    }
                });

        // Monitor the Deployments status.
        // Set up an event handler for when Deployment resources change. This handler will lookup
        // the owner of the given Deployment, and if it is owned by a RemoteShuffleApplication
        // resource will enqueue that RemoteShuffleApplication resource for processing. This way, we
        // don't need to implement custom logic for handling Deployment resources. More info on this
        // pattern:
        // https://github.com/kubernetes/community/blob/8cafef897a22026d42f5e5bb3f104febe7e29830/contributors/devel/controllers.md
        deploymentInformer.addEventHandler(
                new ResourceEventHandler<Deployment>() {
                    @Override
                    public void onAdd(Deployment deployment) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Deployment {} add.",
                                    KubernetesUtils.getResourceFullName(deployment));
                        }
                        handleComponentEvent(deployment, ComponentEventType.ADD);
                    }

                    @Override
                    public void onUpdate(Deployment oldDeployment, Deployment newDeployment) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Deployment {} update.",
                                    KubernetesUtils.getResourceFullName(oldDeployment));
                        }
                        if (!Objects.equals(oldDeployment.getSpec(), newDeployment.getSpec())) {
                            handleComponentEvent(newDeployment, ComponentEventType.UPDATE_SPEC);
                        } else if (!Objects.equals(
                                oldDeployment.getStatus(), newDeployment.getStatus())) {
                            handleComponentEvent(newDeployment, ComponentEventType.UPDATE_STATUS);
                        }
                    }

                    @Override
                    public void onDelete(Deployment deployment, boolean b) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "Deployment {} delete.",
                                    KubernetesUtils.getResourceFullName(deployment));
                        }
                        handleComponentEvent(deployment, ComponentEventType.DELETE);
                    }
                });

        // Monitor the DaemonSets status.
        daemonSetInformer.addEventHandler(
                new ResourceEventHandler<DaemonSet>() {
                    @Override
                    public void onAdd(DaemonSet daemonSet) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "DaemonSet {} add.",
                                    KubernetesUtils.getResourceFullName(daemonSet));
                        }
                        handleComponentEvent(daemonSet, ComponentEventType.ADD);
                    }

                    @Override
                    public void onUpdate(DaemonSet oldDaemonSet, DaemonSet newDaemonSet) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "DaemonSet {} update.",
                                    KubernetesUtils.getResourceFullName(oldDaemonSet));
                        }
                        if (!Objects.equals(oldDaemonSet.getSpec(), newDaemonSet.getSpec())) {
                            handleComponentEvent(newDaemonSet, ComponentEventType.UPDATE_SPEC);
                        } else if (!Objects.equals(
                                oldDaemonSet.getStatus(), newDaemonSet.getStatus())) {
                            handleComponentEvent(newDaemonSet, ComponentEventType.UPDATE_STATUS);
                        }
                    }

                    @Override
                    public void onDelete(DaemonSet daemonSet, boolean b) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                    "DaemonSet {} delete.",
                                    KubernetesUtils.getResourceFullName(daemonSet));
                        }
                        handleComponentEvent(daemonSet, ComponentEventType.DELETE);
                    }
                });
    }

    private void enqueueRemoteShuffleApplication(
            RemoteShuffleApplication shuffleApp, RemoteShuffleAction actionType) {
        String key = Cache.metaNamespaceKeyFunc(shuffleApp);
        if (key != null && !key.isEmpty()) {
            workQueue.add(Pair.of(key, actionType));
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Enqueue RemoteShuffleApplication({}) with key {}, action {}",
                        shuffleApp.getMetadata().getName(),
                        key,
                        actionType);
            }
        }
    }

    public void run() {
        LOG.info("Starting RemoteShuffleApplication controller");

        isRunning.set(true);

        while (!shuffleAppInformer.hasSynced()
                || !deploymentInformer.hasSynced()
                || !daemonSetInformer.hasSynced()) {
            // Wait till Informer syncs
        }

        while (true) {
            try {
                Pair<String, RemoteShuffleAction> shuffleEvent = workQueue.take();
                String key = shuffleEvent.getKey();
                RemoteShuffleAction remoteShuffleAction = shuffleEvent.getValue();
                if (key == null || key.isEmpty() || (!key.contains("/"))) {
                    LOG.info("Invalid resource key: {}", key);
                }

                // Get the RemoteShuffleApplication resource's name from key (in format
                // namespace/name)
                RemoteShuffleApplication shuffleApp = shuffleAppLister.get(key);
                if (shuffleApp == null) {
                    LOG.warn("RemoteShuffleApplication {} in workQueue no longer exists", key);
                } else {
                    if (remoteShuffleAction == RemoteShuffleAction.ADD_OR_UPDATE) {
                        shuffleAppReconciler.reconcile(shuffleApp);
                    }
                    updateRemoteShuffleApplicationStatusWithRetry(shuffleApp);
                }

            } catch (InterruptedException interruptedException) {
                Thread.currentThread().interrupt();
                LOG.info("RemoteShuffleApplication controller interrupted..");
                break;
            } catch (Throwable throwable) {
                LOG.error("Error : ", throwable);
            }
        }
    }

    private void handleComponentEvent(HasMetadata resource, ComponentEventType eventType) {
        OwnerReference ownerReference = KubernetesUtils.getControllerOf(resource);
        if (ownerReference != null
                && ownerReference.getKind().equalsIgnoreCase(RemoteShuffleApplication.KIND)) {
            RemoteShuffleApplication shuffleApp =
                    shuffleAppLister
                            .namespace(resource.getMetadata().getNamespace())
                            .get(ownerReference.getName());
            if (shuffleApp == null) {
                LOG.warn(
                        "Receive ShuffleManager/ShuffleWorkers event : {}({}) {} of an unknown shuffle application {}.",
                        resource.getClass().getSimpleName(),
                        KubernetesUtils.getResourceFullName(resource),
                        eventType,
                        ownerReference.getName());
            } else {
                if (eventType == ComponentEventType.UPDATE_STATUS) {
                    enqueueRemoteShuffleApplication(shuffleApp, RemoteShuffleAction.UPDATE_STATUS);
                } else {
                    LOG.info(
                            "Receive ShuffleManager/ShuffleWorkers event : {}({}) {}.",
                            resource.getClass().getSimpleName(),
                            KubernetesUtils.getResourceFullName(resource),
                            eventType);
                    if (eventType == ComponentEventType.ADD) {
                        // do nothing.
                    } else {
                        enqueueRemoteShuffleApplication(
                                shuffleApp, RemoteShuffleAction.ADD_OR_UPDATE);
                    }
                }
            }
        }
    }

    private void updateRemoteShuffleApplicationStatusWithRetry(
            RemoteShuffleApplication shuffleApp) {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastUpdateStatusTime > UPDATE_STATUS_INTERVAL_MS) {
            KubernetesUtils.executeWithRetry(
                    () -> updateRemoteShuffleApplicationStatus(shuffleApp),
                    String.format(
                            "Update %s status", KubernetesUtils.getResourceFullName(shuffleApp)));
            lastUpdateStatusTime = currentTime;
        }
    }

    /**
     * Query the status of each component of {@link RemoteShuffleApplication}, and update the status
     * of remote shuffle application based on components' statuses.
     */
    private void updateRemoteShuffleApplicationStatus(RemoteShuffleApplication shuffleApp) {

        String namespace = shuffleApp.getMetadata().getNamespace();
        String clusterId = shuffleApp.getMetadata().getName();

        String shuffleManagerName =
                String.format(
                        "%s/%s",
                        namespace, KubernetesUtils.getShuffleManagerNameWithClusterId(clusterId));
        String shuffleWorkersName =
                String.format(
                        "%s/%s",
                        namespace, KubernetesUtils.getShuffleWorkersNameWithClusterId(clusterId));

        Deployment deployment = deploymentLister.get(shuffleManagerName);
        DaemonSet daemonSet = daemonSetLister.get(shuffleWorkersName);

        RemoteShuffleApplicationStatus currentStatus = new RemoteShuffleApplicationStatus();

        if (deployment != null && deployment.getStatus() != null) {
            DeploymentStatus status = deployment.getStatus();
            if (status.getReplicas() != null) {
                currentStatus.setDesiredShuffleManagers(status.getReplicas());
            }
            if (status.getReadyReplicas() != null) {
                currentStatus.setReadyShuffleManagers(status.getReadyReplicas());
            }
        }

        if (daemonSet != null && daemonSet.getStatus() != null) {
            DaemonSetStatus status = daemonSet.getStatus();
            if (status.getDesiredNumberScheduled() != null) {
                currentStatus.setDesiredShuffleWorkers(status.getDesiredNumberScheduled());
            }
            if (status.getNumberReady() != null) {
                currentStatus.setReadyShuffleWorkers(status.getNumberReady());
            }
        }

        RemoteShuffleApplication cloneShuffleApp = cloneRemoteShuffleApplication(shuffleApp);
        cloneShuffleApp.setStatus(currentStatus);
        shuffleAppClient.inNamespace(namespace).withName(clusterId).updateStatus(cloneShuffleApp);
    }

    private static RemoteShuffleApplication cloneRemoteShuffleApplication(
            RemoteShuffleApplication shuffleApp) {
        RemoteShuffleApplication cloneShuffleApp = new RemoteShuffleApplication();
        RemoteShuffleApplicationSpec cloneShuffleSpec =
                new RemoteShuffleApplicationSpec(
                        shuffleApp.getSpec().getShuffleDynamicConfigs(),
                        shuffleApp.getSpec().getShuffleFileConfigs());

        cloneShuffleApp.setSpec(cloneShuffleSpec);
        cloneShuffleApp.setMetadata(shuffleApp.getMetadata());

        return cloneShuffleApp;
    }

    static Deployment cloneResource(Deployment deployment) throws IOException {
        final ObjectMapper jsonMapper = Serialization.jsonMapper();
        byte[] bytes = jsonMapper.writeValueAsBytes(deployment);
        return jsonMapper.readValue(bytes, Deployment.class);
    }

    static DaemonSet cloneResource(DaemonSet daemonSet) throws IOException {
        final ObjectMapper jsonMapper = Serialization.jsonMapper();
        byte[] bytes = jsonMapper.writeValueAsBytes(daemonSet);
        return jsonMapper.readValue(bytes, DaemonSet.class);
    }

    public static RemoteShuffleApplicationController createRemoteShuffleApplicationController(
            KubernetesClient kubeClient, SharedInformerFactory informerFactory) {
        // create informers.
        final SharedIndexInformer<RemoteShuffleApplication> shuffleAppInformer =
                informerFactory.sharedIndexInformerForCustomResource(
                        RemoteShuffleApplication.class, RSYNC_MILLIS);
        final SharedIndexInformer<Deployment> deploymentInformer =
                informerFactory.sharedIndexInformerFor(Deployment.class, RSYNC_MILLIS);
        final SharedIndexInformer<DaemonSet> daemonSetInformer =
                informerFactory.sharedIndexInformerFor(DaemonSet.class, RSYNC_MILLIS);

        MixedOperation<
                        RemoteShuffleApplication,
                        RemoteShuffleApplicationList,
                        Resource<RemoteShuffleApplication>>
                shuffleAppClient =
                        kubeClient.customResources(
                                RemoteShuffleApplication.class, RemoteShuffleApplicationList.class);

        // create shuffle application controller.
        RemoteShuffleApplicationController remoteShuffleApplicationController =
                new RemoteShuffleApplicationController(
                        shuffleAppInformer,
                        deploymentInformer,
                        daemonSetInformer,
                        kubeClient,
                        shuffleAppClient);
        remoteShuffleApplicationController.create();

        return remoteShuffleApplicationController;
    }

    // ---------------------------------------------------------------------------------------------
    // For test
    // ---------------------------------------------------------------------------------------------

    SharedIndexInformer<Deployment> getDeploymentInformer() {
        return deploymentInformer;
    }

    SharedIndexInformer<DaemonSet> getDaemonSetInformer() {
        return daemonSetInformer;
    }

    Lister<RemoteShuffleApplication> getShuffleAppLister() {
        return shuffleAppLister;
    }

    Lister<DaemonSet> getDaemonSetLister() {
        return daemonSetLister;
    }

    Lister<Deployment> getDeploymentLister() {
        return deploymentLister;
    }
}
