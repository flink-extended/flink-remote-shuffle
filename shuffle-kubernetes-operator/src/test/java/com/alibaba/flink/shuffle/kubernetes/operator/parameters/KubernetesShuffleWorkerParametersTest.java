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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesInternalOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesUtils;

import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesShuffleWorkerParameters}. */
public class KubernetesShuffleWorkerParametersTest extends KubernetesTestBase {

    private Configuration conf;
    private KubernetesShuffleWorkerParameters shuffleWorkerParameters;

    @Before
    public void setup() {
        conf = new Configuration();

        // cluster id and namespace
        conf.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, CLUSTER_ID);
        conf.setString(KubernetesInternalOptions.NAMESPACE, NAMESPACE);

        // memory config
        conf.setMemorySize(
                WorkerOptions.FRAMEWORK_HEAP_MEMORY,
                MemorySize.parse(CONTAINER_FRAMEWORK_HEAP_MEMORY_MB + "m"));
        conf.setMemorySize(
                WorkerOptions.FRAMEWORK_OFF_HEAP_MEMORY,
                MemorySize.parse(CONTAINER_FRAMEWORK_OFF_HEAP_MEMORY_MB + "m"));
        conf.setMemorySize(MemoryOptions.MEMORY_BUFFER_SIZE, NETWORK_MEMORY_BUFFER_SIZE);
        conf.setMemorySize(MemoryOptions.MEMORY_SIZE_FOR_DATA_READING, NETWORK_READING_MEMORY_SIZE);
        conf.setMemorySize(MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING, NETWORK_WRITING_MEMORY_SIZE);
        conf.setMemorySize(
                WorkerOptions.JVM_METASPACE, MemorySize.parse(CONTAINER_JVM_METASPACE_MB + "m"));
        conf.setMemorySize(
                WorkerOptions.JVM_OVERHEAD, MemorySize.parse(CONTAINER_JVM_OVERHEAD_MB + "m"));
        conf.setInteger(KubernetesOptions.SHUFFLE_WORKER_REPLICAS, WORKER_REPLICAS);
        conf.setString(KubernetesOptions.SHUFFLE_WORKER_PVC_STORAGE_SIZE, PVC_STORAGE_SIZE);
        conf.setString(KubernetesOptions.SHUFFLE_WORKER_PVC_STORAGE_CLASS, STORAGE_CLASS);
        conf.setList(KubernetesOptions.SHUFFLE_WORKER_PVC_ACCESS_MODE, ACCESS_MODE);
        conf.setString(KubernetesOptions.SHUFFLE_WORKER_PVC_MOUNT_PATH, MOUNT_PATH);

        shuffleWorkerParameters = new KubernetesShuffleWorkerParameters(conf);
    }

    @Test
    public void testGetLabels() {
        conf.setMap(KubernetesOptions.SHUFFLE_WORKER_LABELS, USER_LABELS);

        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_SHUFFLE_WORKER);
        expectedLabels.putAll(USER_LABELS);
        assertEquals(shuffleWorkerParameters.getLabels(), expectedLabels);
    }

    @Test
    public void testGetNodeSelector() {
        conf.setMap(KubernetesOptions.SHUFFLE_WORKER_NODE_SELECTOR, NODE_SELECTOR);
        assertEquals(shuffleWorkerParameters.getNodeSelector(), NODE_SELECTOR);
    }

    @Test
    public void testGetContainerName() {
        MatcherAssert.assertThat(
                shuffleWorkerParameters.getContainerName(),
                CoreMatchers.is(KubernetesUtils.SHUFFLE_WORKER_CONTAINER_NAME));
    }

    @Test
    public void testGetContainerMemoryMB() {
        assertThat(shuffleWorkerParameters.getContainerMemoryMB(), is(getTotalMemory(true)));
    }

    @Test
    public void testGetContainerCPU() {
        conf.setDouble(KubernetesOptions.SHUFFLE_WORKER_CPU, CONTAINER_CPU);
        assertThat(shuffleWorkerParameters.getContainerCPU(), is(CONTAINER_CPU));
    }

    @Test
    public void testGetResourceLimitFactors() {
        final Map<String, String> limitFactors = new HashMap<>();
        limitFactors.put("cpu", "3.0");
        limitFactors.put("memory", "1.5");

        conf.setString("remote-shuffle.kubernetes.worker.limit-factor.cpu", "3.0");
        conf.setString("remote-shuffle.kubernetes.worker.limit-factor.memory", "1.5");

        assertEquals(shuffleWorkerParameters.getResourceLimitFactors(), limitFactors);
    }

    @Test
    public void testGetContainerVolumeMounts() {
        conf.setList(KubernetesOptions.SHUFFLE_WORKER_HOST_PATH_VOLUMES, HOST_PATH_VOLUMES);
        assertEquals(shuffleWorkerParameters.getContainerVolumeMounts(), CONTAINER_VOLUME_MOUNTS);
    }

    @Test
    public void testGetContainerCommandAndArgs() {
        conf.setString(WorkerOptions.JVM_OPTIONS, CONTAINER_JVM_OPTIONS);

        ContainerCommandAndArgs commandAndArgs =
                shuffleWorkerParameters.getContainerCommandAndArgs();

        assertThat("bash", is(commandAndArgs.getCommand()));
        assertEquals(
                commandAndArgs.getArgs(),
                Arrays.asList(
                        "-c",
                        "/flink-remote-shuffle/bin/kubernetes-shuffleworker.sh"
                                + " -D 'remote-shuffle.cluster.id=TestingCluster'"
                                + " -D 'remote-shuffle.memory.buffer-size=1 mb'"
                                + " -D 'remote-shuffle.memory.data-reading-size=32 mb'"
                                + " -D 'remote-shuffle.memory.data-writing-size=32 mb'"
                                + " -D 'remote-shuffle.worker.jvm-opts=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4'"
                                + " -D 'remote-shuffle.worker.memory.heap-size=256 mb'"
                                + " -D 'remote-shuffle.worker.memory.jvm-metaspace-size=32 mb'"
                                + " -D 'remote-shuffle.worker.memory.jvm-overhead-size=32 mb'"
                                + " -D 'remote-shuffle.worker.memory.off-heap-size=128 mb'"));
    }

    @Test
    public void testGetDaemonSetName() {
        assertThat(shuffleWorkerParameters.getDaemonSetName(), is(CLUSTER_ID + "-shuffleworker"));
    }

    @Test
    public void testStatefulSetParameters() {
        assertThat(shuffleWorkerParameters.getStatefulSetName(), is(CLUSTER_ID + "-shuffleworker"));
        assertThat(shuffleWorkerParameters.getReplicas(), is(WORKER_REPLICAS));
        assertThat(shuffleWorkerParameters.getStorageResource(), is(STORAGE_RESOURCES));
        assertThat(shuffleWorkerParameters.getStorageClass(), is(STORAGE_CLASS));
        assertThat(shuffleWorkerParameters.getAccessMode(), is(ACCESS_MODE));
        assertThat(shuffleWorkerParameters.getMountPath(), is(MOUNT_PATH));
    }
}
