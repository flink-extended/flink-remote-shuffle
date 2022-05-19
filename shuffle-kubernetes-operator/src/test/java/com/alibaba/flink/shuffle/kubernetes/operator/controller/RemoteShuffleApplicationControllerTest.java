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

import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplication;
import com.alibaba.flink.shuffle.kubernetes.operator.crd.RemoteShuffleApplicationSpec;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDaemonSetParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.KubernetesDeploymentParameters;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesDaemonSetBuilder;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesDaemonSetBuilderTest;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesDeploymentBuilder;
import com.alibaba.flink.shuffle.kubernetes.operator.resources.KubernetesDeploymentBuilderTest;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesInternalOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.apps.DaemonSet;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedInformerFactory;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test for {@link RemoteShuffleApplicationController}. */
public class RemoteShuffleApplicationControllerTest extends KubernetesTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoteShuffleApplicationControllerTest.class);

    private RemoteShuffleApplicationController shuffleAppController;
    private ScheduledExecutorService shuffleAppControllerExecutor;
    private KubernetesClient kubeClient;

    static Map<String, List<HasMetadata>> deleteEvents = new HashMap<>();
    static Map<String, List<HasMetadata>> addEvents = new HashMap<>();
    static Map<String, List<HasMetadata>> updateEvents = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        addEvents.clear();
        updateEvents.clear();
        deleteEvents.clear();

        // setup client and resource event handler.
        KubernetesServer server =
                new KubernetesServer(
                        false,
                        true,
                        InetAddress.getByName("127.0.0.1"),
                        0,
                        Collections.emptyList());
        server.before();
        kubeClient = server.getClient();
        ControllerRunner controllerRunner = new ControllerRunner(kubeClient);
        controllerRunner.registerShuffleManagerResourceEventHandler(
                new RecordResourceEventHandler<>());
        controllerRunner.registerShuffleWorkersResourceEventHandler(
                new RecordResourceEventHandler<>());
        shuffleAppController = controllerRunner.getRemoteShuffleApplicationController();
        shuffleAppControllerExecutor = Executors.newSingleThreadScheduledExecutor();
        shuffleAppControllerExecutor.schedule(controllerRunner, 0, TimeUnit.MILLISECONDS);
        waitForControllerReady();
    }

    @After
    public void cleanUp() throws Exception {
        shuffleAppControllerExecutor.shutdownNow();
    }

    private static class RecordResourceEventHandler<T> implements ResourceEventHandler<T> {

        @Override
        public void onAdd(T t) {
            checkState(t instanceof HasMetadata);
            HasMetadata resource = (HasMetadata) t;
            recordEvents(addEvents, resource);
        }

        @Override
        public void onUpdate(T t, T t1) {
            checkState(t instanceof HasMetadata);
            HasMetadata resource = (HasMetadata) t1;
            recordEvents(updateEvents, resource);
        }

        @Override
        public void onDelete(T t, boolean b) {
            checkState(t instanceof HasMetadata);
            HasMetadata resource = (HasMetadata) t;
            recordEvents(deleteEvents, resource);
        }
    }

    private static void recordEvents(Map<String, List<HasMetadata>> events, HasMetadata resource) {
        events.compute(
                resource.getKind(),
                (kind, list) -> {
                    final List<HasMetadata> hasMetadataList;
                    if (list != null) {
                        hasMetadataList = list;
                    } else {
                        hasMetadataList = new ArrayList<>();
                    }
                    hasMetadataList.add(resource);
                    return hasMetadataList;
                });
    }

    private void waitUntilResourceReady(Class<?> clazz, String resourceName) throws Exception {
        KubernetesUtils.waitUntilCondition(
                () -> checkResourceReady(clazz, resourceName), Duration.ofMinutes(2));
    }

    private boolean checkResourceReady(Class<?> clazz, String resourceName) {
        if (clazz == Deployment.class) {
            return getDeploymentList().stream()
                    .map(deployment -> deployment.getMetadata().getName())
                    .collect(Collectors.toList())
                    .contains(resourceName);
        } else if (clazz == DaemonSet.class) {
            return getDaemonSetList().stream()
                    .map(daemonSet -> daemonSet.getMetadata().getName())
                    .collect(Collectors.toList())
                    .contains(resourceName);
        } else if (clazz == RemoteShuffleApplication.class) {
            return getRemoteShuffleApplicationList().stream()
                    .map(shuffleApplication -> shuffleApplication.getMetadata().getName())
                    .collect(Collectors.toList())
                    .contains(resourceName);
        } else {
            throw new UnsupportedOperationException();
        }
    }

    private void waitForControllerReady() throws Exception {
        KubernetesUtils.waitUntilCondition(
                () -> shuffleAppController.isRunning.get(), Duration.ofSeconds(30));
    }

    private void deployRemoteShuffleApplication(RemoteShuffleApplication shuffleApp) {
        kubeClient
                .customResources(RemoteShuffleApplication.class)
                .inNamespace(NAMESPACE)
                .createOrReplace(shuffleApp);
    }

    private void deleteRemoteShuffleApplication(RemoteShuffleApplication shuffleApp) {
        kubeClient
                .customResources(RemoteShuffleApplication.class)
                .inNamespace(NAMESPACE)
                .delete(shuffleApp);
    }

    private List<RemoteShuffleApplication> getRemoteShuffleApplicationList() {
        return shuffleAppController.getShuffleAppLister().namespace(NAMESPACE).list();
    }

    private RemoteShuffleApplication getRemoteShuffleApplication(String name) {
        return shuffleAppController.getShuffleAppLister().namespace(NAMESPACE).get(name);
    }

    private List<Deployment> getDeploymentList() {
        return shuffleAppController.getDeploymentLister().namespace(NAMESPACE).list();
    }

    private List<DaemonSet> getDaemonSetList() {
        return shuffleAppController.getDaemonSetLister().namespace(NAMESPACE).list();
    }

    private Deployment getDeployment(String name) {
        return shuffleAppController.getDeploymentLister().namespace(NAMESPACE).get(name);
    }

    private DaemonSet getDaemonSet(String name) {
        return shuffleAppController.getDaemonSetLister().namespace(NAMESPACE).get(name);
    }

    private void deleteDeployment(Deployment deployment) {
        kubeClient.apps().deployments().inNamespace(NAMESPACE).delete(deployment);
    }

    private void deleteDaemonSet(DaemonSet daemonSet) {
        kubeClient.apps().daemonSets().inNamespace(NAMESPACE).delete(daemonSet);
    }

    private void deployDeployment(Deployment deployment) {
        kubeClient.apps().deployments().inNamespace(NAMESPACE).createOrReplace(deployment);
    }

    private void deployDaemonSet(DaemonSet daemonSet) {
        kubeClient.apps().daemonSets().inNamespace(NAMESPACE).createOrReplace(daemonSet);
    }

    private String getShuffleManagerName(RemoteShuffleApplication remoteShuffleApplication) {
        return KubernetesUtils.getShuffleManagerNameWithClusterId(
                remoteShuffleApplication.getMetadata().getName());
    }

    private String getShuffleWorkersName(RemoteShuffleApplication remoteShuffleApplication) {
        return KubernetesUtils.getShuffleWorkersNameWithClusterId(
                remoteShuffleApplication.getMetadata().getName());
    }

    private RemoteShuffleApplication createRemoteShuffleApplication() throws Exception {
        Map<String, String> dynamicConfigs = createDynamicConfigs();
        Map<String, String> fileConfigs = createFileConfigs();
        return createRemoteShuffleApplication(dynamicConfigs, fileConfigs);
    }

    private RemoteShuffleApplication createRemoteShuffleApplication(
            Map<String, String> dynamicConfigs, Map<String, String> fileConfigs) throws Exception {

        RemoteShuffleApplicationSpec remoteShuffleApplicationSpec =
                new RemoteShuffleApplicationSpec();
        remoteShuffleApplicationSpec.setShuffleDynamicConfigs(dynamicConfigs);
        remoteShuffleApplicationSpec.setShuffleFileConfigs(fileConfigs);

        RemoteShuffleApplication remoteShuffleApplication = new RemoteShuffleApplication();
        ObjectMeta metadata =
                new ObjectMetaBuilder().withNamespace(NAMESPACE).withName(CLUSTER_ID).build();
        remoteShuffleApplication.setMetadata(metadata);
        remoteShuffleApplication.setSpec(remoteShuffleApplicationSpec);

        deployRemoteShuffleApplication(remoteShuffleApplication);
        waitUntilResourceReady(
                RemoteShuffleApplication.class, remoteShuffleApplication.getMetadata().getName());
        // check shuffle manager and shuffle worker.
        waitUntilResourceReady(Deployment.class, getShuffleManagerName(remoteShuffleApplication));
        waitUntilResourceReady(DaemonSet.class, getShuffleWorkersName(remoteShuffleApplication));

        List<RemoteShuffleApplication> shuffleApps = getRemoteShuffleApplicationList();
        assertThat(shuffleApps.size(), is(1));
        assertThat(shuffleApps.get(0).getMetadata(), is(remoteShuffleApplication.getMetadata()));
        assertThat(shuffleApps.get(0).getSpec(), is(remoteShuffleApplication.getSpec()));

        return remoteShuffleApplication;
    }

    private Map<String, String> createDynamicConfigs() {
        Map<String, String> dynamicConfigs = new HashMap<>();
        dynamicConfigs.put(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.key(), CLUSTER_ID);
        dynamicConfigs.put(KubernetesInternalOptions.NAMESPACE.key(), NAMESPACE);
        dynamicConfigs.put(KubernetesOptions.CONTAINER_IMAGE.key(), CONTAINER_IMAGE);
        dynamicConfigs.put(KubernetesOptions.POD_HOST_NETWORK_ENABLED.key(), "true");
        dynamicConfigs.put(StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(), "/data");
        dynamicConfigs.put(TransferOptions.SERVER_DATA_PORT.key(), "10085");
        dynamicConfigs.put(HighAvailabilityOptions.HA_MODE.key(), "ZOOKEEPER");
        dynamicConfigs.put(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key(), "localhost");
        // shuffle manager configs
        dynamicConfigs.put(KubernetesOptions.SHUFFLE_MANAGER_CPU.key(), "2");
        dynamicConfigs.put(ManagerOptions.FRAMEWORK_HEAP_MEMORY.key(), "256mb");
        // shuffle worker configs
        dynamicConfigs.put(KubernetesOptions.SHUFFLE_WORKER_CPU.key(), "2");
        dynamicConfigs.put(WorkerOptions.FRAMEWORK_HEAP_MEMORY.key(), "256mb");

        return dynamicConfigs;
    }

    private Map<String, String> createFileConfigs() {
        Map<String, String> fileConfigs = new HashMap<>();
        fileConfigs.put("file1", "This is a test for config file.");
        fileConfigs.put("file2", "This is a test for config file.");
        return fileConfigs;
    }

    @Ignore("Temporarily ignore.")
    @Test(timeout = 60000L)
    public void testShuffleApplicationAddAndDelete() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication = createRemoteShuffleApplication();

        // delete shuffle application
        deleteRemoteShuffleApplication(remoteShuffleApplication);
        KubernetesUtils.waitUntilCondition(
                () -> getRemoteShuffleApplicationList().size() == 0, Duration.ofSeconds(30));
    }

    @Ignore("Temporarily ignore.")
    @Test(timeout = 60000L)
    public void testShuffleApplicationUpdate() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication = createRemoteShuffleApplication();

        remoteShuffleApplication
                .getSpec()
                .getShuffleDynamicConfigs()
                .put(KubernetesOptions.CONTAINER_IMAGE.key(), "shuffleApp:666");
        deployRemoteShuffleApplication(remoteShuffleApplication);

        KubernetesUtils.waitUntilCondition(
                () -> {
                    RemoteShuffleApplication shuffleApp =
                            getRemoteShuffleApplication(
                                    remoteShuffleApplication.getMetadata().getName());
                    assertNotNull(shuffleApp);
                    return shuffleApp
                            .getSpec()
                            .getShuffleDynamicConfigs()
                            .get(KubernetesOptions.CONTAINER_IMAGE.key())
                            .equals("shuffleApp:666");
                },
                Duration.ofSeconds(30));
        // check update image in shuffle manager.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    Deployment shuffleManager =
                            getDeployment(getShuffleManagerName(remoteShuffleApplication));
                    assertNotNull(shuffleManager);
                    return shuffleManager
                            .getSpec()
                            .getTemplate()
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getImage()
                            .equals("shuffleApp:666");
                },
                Duration.ofSeconds(30));
        // check update image in shuffle workers.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    DaemonSet shuffleWorkers =
                            getDaemonSet(getShuffleWorkersName(remoteShuffleApplication));
                    assertNotNull(shuffleWorkers);
                    return shuffleWorkers
                            .getSpec()
                            .getTemplate()
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getImage()
                            .equals("shuffleApp:666");
                },
                Duration.ofSeconds(30));
    }

    @Ignore("Temporarily ignore.")
    @Test(timeout = 60000L)
    public void testShuffleManagerUpdate() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication = createRemoteShuffleApplication();

        // update shuffle manager.
        Deployment newDeployment =
                RemoteShuffleApplicationController.cloneResource(
                        getDeployment(getShuffleManagerName(remoteShuffleApplication)));
        newDeployment.getSpec().setReplicas(10);
        deployDeployment(newDeployment);
        String newDeploymentName = newDeployment.getMetadata().getName();

        // check the replicas has been updated to 10.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    List<HasMetadata> resources = updateEvents.get("Deployment");
                    if (resources == null) {
                        return false;
                    }
                    for (HasMetadata resource : resources) {
                        Deployment deployment = (Deployment) resource;
                        if (deployment.getMetadata().getName().equals(newDeploymentName)
                                && deployment.getSpec().getReplicas().intValue() == 10) {
                            return true;
                        }
                    }
                    return false;
                },
                Duration.ofSeconds(30));

        // check the replicas be reconciled to 1.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    Deployment deployment = getDeployment(newDeploymentName);
                    assertNotNull(deployment);
                    return deployment.getSpec().getReplicas().intValue() == 1;
                },
                Duration.ofSeconds(30));
    }

    @Ignore("Temporarily ignore.")
    @Test(timeout = 60000L)
    public void testShuffleManagerDelete() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication = createRemoteShuffleApplication();

        // delete shuffle manager.
        Deployment deployment = getDeployment(getShuffleManagerName(remoteShuffleApplication));
        deleteDeployment(deployment);

        // check the shuffle manager has been delete.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    List<HasMetadata> resources = deleteEvents.get("Deployment");
                    if (resources == null) {
                        return false;
                    }

                    return resources.stream()
                            .map(resource -> resource.getMetadata().getName())
                            .collect(Collectors.toList())
                            .contains(deployment.getMetadata().getName());
                },
                Duration.ofSeconds(30));

        // check the shuffle manager be reconciled.
        KubernetesUtils.waitUntilCondition(
                () ->
                        getDeploymentList().size() == 1
                                && getDeploymentList()
                                        .get(0)
                                        .getMetadata()
                                        .getName()
                                        .equals(deployment.getMetadata().getName()),
                Duration.ofSeconds(30));
    }

    @Ignore("Temporarily ignore.")
    @Test(timeout = 60000L)
    public void testShuffleWorkersUpdate() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication = createRemoteShuffleApplication();

        // update shuffle workers.
        DaemonSet newDaemonSet =
                RemoteShuffleApplicationController.cloneResource(
                        getDaemonSet(getShuffleWorkersName(remoteShuffleApplication)));
        newDaemonSet
                .getSpec()
                .getTemplate()
                .getSpec()
                .getContainers()
                .get(0)
                .setImage("shuffleApp:666");
        deployDaemonSet(newDaemonSet);
        String newDaemonSetName = newDaemonSet.getMetadata().getName();

        // check the image has been updated to shuffleApp:666.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    List<HasMetadata> resources = updateEvents.get("DaemonSet");
                    if (resources == null) {
                        return false;
                    }
                    for (HasMetadata resource : resources) {
                        DaemonSet daemonSet = (DaemonSet) resource;
                        if (daemonSet.getMetadata().getName().equals(newDaemonSetName)
                                && daemonSet
                                        .getSpec()
                                        .getTemplate()
                                        .getSpec()
                                        .getContainers()
                                        .get(0)
                                        .getImage()
                                        .equals("shuffleApp:666")) {
                            return true;
                        }
                    }
                    return false;
                },
                Duration.ofSeconds(30));

        // check the image be reconciled to flink-remote-shuffle-k8s-test:latest.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    DaemonSet daemonSet = getDaemonSet(newDaemonSetName);
                    assertNotNull(daemonSet);
                    return daemonSet
                            .getSpec()
                            .getTemplate()
                            .getSpec()
                            .getContainers()
                            .get(0)
                            .getImage()
                            .equals(CONTAINER_IMAGE);
                },
                Duration.ofSeconds(30));
    }

    @Ignore("Temporarily ignore.")
    @Test(timeout = 60000L)
    public void testShuffleWorkersDelete() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication = createRemoteShuffleApplication();

        // delete shuffle workers.
        DaemonSet daemonSet = getDaemonSet(getShuffleWorkersName(remoteShuffleApplication));
        deleteDaemonSet(daemonSet);

        // check the shuffle workers has been deleted.
        KubernetesUtils.waitUntilCondition(
                () -> {
                    List<HasMetadata> resources = deleteEvents.get("DaemonSet");
                    if (resources == null) {
                        return false;
                    }

                    return resources.stream()
                            .map(resource -> resource.getMetadata().getName())
                            .collect(Collectors.toList())
                            .contains(daemonSet.getMetadata().getName());
                },
                Duration.ofSeconds(30));

        // check the shuffle workers be reconciled.
        KubernetesUtils.waitUntilCondition(
                () ->
                        getDaemonSetList().size() == 1
                                && getDaemonSetList()
                                        .get(0)
                                        .getMetadata()
                                        .getName()
                                        .equals(daemonSet.getMetadata().getName()),
                Duration.ofSeconds(30));
    }

    @Ignore
    @Test(timeout = 60000L)
    public void testNullFileConfigs() throws Exception {
        // create a new shuffle application
        RemoteShuffleApplication remoteShuffleApplication =
                createRemoteShuffleApplication(createDynamicConfigs(), null);
        // no configmap.
        assertThat(kubeClient.configMaps().inNamespace(NAMESPACE).list().getItems().size(), is(0));
    }

    @Test
    public void testCloneDeployment() throws IOException {
        KubernetesDeploymentParameters deploymentParameters =
                new KubernetesDeploymentBuilderTest.TestingDeploymentParameters();
        Deployment deployment =
                new KubernetesDeploymentBuilder().buildKubernetesResourceFrom(deploymentParameters);
        Deployment cloneDeployment = RemoteShuffleApplicationController.cloneResource(deployment);
        assertEquals(cloneDeployment, deployment);
    }

    @Test
    public void testCloneDaemonSet() throws IOException {
        KubernetesDaemonSetParameters daemonSetParameters =
                new KubernetesDaemonSetBuilderTest.TestingDaemonSetParameters();
        DaemonSet daemonSet =
                new KubernetesDaemonSetBuilder().buildKubernetesResourceFrom(daemonSetParameters);
        DaemonSet cloneDaemonSet = RemoteShuffleApplicationController.cloneResource(daemonSet);
        assertEquals(cloneDaemonSet, daemonSet);
    }

    /** Runner class for running a shuffle application controller. */
    private static class ControllerRunner implements Runnable {

        public final RemoteShuffleApplicationController remoteShuffleApplicationController;
        public final SharedInformerFactory informerFactory;
        public final KubernetesClient kubeClient;
        public final ExecutorService executorPool =
                Executors.newFixedThreadPool(5, new ExecutorThreadFactory("informers"));

        public ControllerRunner(KubernetesClient kubeClient) {
            this.kubeClient = kubeClient;
            this.informerFactory = kubeClient.informers(executorPool);
            this.remoteShuffleApplicationController =
                    RemoteShuffleApplicationController.createRemoteShuffleApplicationController(
                            kubeClient, informerFactory);
        }

        public void registerShuffleManagerResourceEventHandler(
                ResourceEventHandler<Deployment> eventHandler) {
            remoteShuffleApplicationController
                    .getDeploymentInformer()
                    .addEventHandler(eventHandler);
        }

        public void registerShuffleWorkersResourceEventHandler(
                ResourceEventHandler<DaemonSet> eventHandler) {
            remoteShuffleApplicationController.getDaemonSetInformer().addEventHandler(eventHandler);
        }

        @Override
        public void run() {
            try {
                informerFactory.startAllRegisteredInformers();
                informerFactory.addSharedInformerEventListener(
                        exception -> LOG.error("Exception occurred, but caught", exception));
                remoteShuffleApplicationController.run();
            } catch (Throwable throwable) {
                LOG.error("Shuffle application operator failed.", throwable);
            } finally {
                executorPool.shutdownNow();
                informerFactory.stopAllRegisteredInformers();
            }
        }

        public RemoteShuffleApplicationController getRemoteShuffleApplicationController() {
            return remoteShuffleApplicationController;
        }
    }
}
