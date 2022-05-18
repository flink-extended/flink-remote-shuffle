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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesInternalOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesTestBase;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Test for {@link KubernetesShuffleManagerParameters}. */
public class KubernetesShuffleManagerParametersTest extends KubernetesTestBase {

    private Configuration conf;
    private KubernetesShuffleManagerParameters shuffleManagerParameters;

    @Before
    public void setup() {
        conf = new Configuration();

        // cluster id and namespace
        conf.setString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID, CLUSTER_ID);
        conf.setString(KubernetesInternalOptions.NAMESPACE, NAMESPACE);

        // memory config
        conf.setMemorySize(
                ManagerOptions.FRAMEWORK_HEAP_MEMORY,
                MemorySize.parse(CONTAINER_FRAMEWORK_HEAP_MEMORY_MB + "m"));
        conf.setMemorySize(
                ManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY,
                MemorySize.parse(CONTAINER_FRAMEWORK_OFF_HEAP_MEMORY_MB + "m"));
        conf.setMemorySize(
                ManagerOptions.JVM_METASPACE, MemorySize.parse(CONTAINER_JVM_METASPACE_MB + "m"));
        conf.setMemorySize(
                ManagerOptions.JVM_OVERHEAD, MemorySize.parse(CONTAINER_JVM_OVERHEAD_MB + "m"));

        shuffleManagerParameters = new KubernetesShuffleManagerParameters(conf);
    }

    @Test
    public void testGetLabels() {

        conf.setMap(KubernetesOptions.SHUFFLE_MANAGER_LABELS, USER_LABELS);

        final Map<String, String> expectedLabels = new HashMap<>(getCommonLabels());
        expectedLabels.put(
                Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_SHUFFLE_MANAGER);
        expectedLabels.putAll(USER_LABELS);
        assertEquals(shuffleManagerParameters.getLabels(), expectedLabels);
    }

    @Test
    public void testGetNodeSelector() {
        conf.setMap(KubernetesOptions.SHUFFLE_MANAGER_NODE_SELECTOR, NODE_SELECTOR);
        assertEquals(shuffleManagerParameters.getNodeSelector(), NODE_SELECTOR);
    }

    @Test
    public void testGetContainerVolumeMounts() {
        conf.setList(KubernetesOptions.SHUFFLE_MANAGER_HOST_PATH_VOLUMES, HOST_PATH_VOLUMES);
        assertEquals(shuffleManagerParameters.getContainerVolumeMounts(), CONTAINER_VOLUME_MOUNTS);
    }

    @Test
    public void testGetContainerMemoryMB() {
        assertThat(shuffleManagerParameters.getContainerMemoryMB(), is(getTotalMemory(false)));
    }

    @Test
    public void testGetContainerCPU() {
        conf.setDouble(KubernetesOptions.SHUFFLE_MANAGER_CPU, CONTAINER_CPU);
        assertThat(shuffleManagerParameters.getContainerCPU(), is(CONTAINER_CPU));
    }

    @Test
    public void testGetResourceLimitFactors() {
        final Map<String, String> limitFactors = new HashMap<>();
        limitFactors.put("cpu", "3.2");
        limitFactors.put("memory", "1.6");

        conf.setString("remote-shuffle.kubernetes.manager.limit-factor.cpu", "3.2");
        conf.setString("remote-shuffle.kubernetes.manager.limit-factor.memory", "1.6");

        assertEquals(shuffleManagerParameters.getResourceLimitFactors(), limitFactors);
    }

    @Test
    public void testGetContainerCommandAndArgs() {

        conf.setString(ManagerOptions.JVM_OPTIONS, CONTAINER_JVM_OPTIONS);

        ContainerCommandAndArgs commandAndArgs =
                shuffleManagerParameters.getContainerCommandAndArgs();

        assertThat("bash", is(commandAndArgs.getCommand()));
        assertEquals(
                commandAndArgs.getArgs(),
                Arrays.asList(
                        "-c",
                        "/flink-remote-shuffle/bin/kubernetes-shufflemanager.sh"
                                + " -D 'remote-shuffle.cluster.id=TestingCluster'"
                                + " -D 'remote-shuffle.manager.jvm-opts=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4'"
                                + " -D 'remote-shuffle.manager.memory.heap-size=256 mb'"
                                + " -D 'remote-shuffle.manager.memory.jvm-metaspace-size=32 mb'"
                                + " -D 'remote-shuffle.manager.memory.jvm-overhead-size=32 mb'"
                                + " -D 'remote-shuffle.manager.memory.off-heap-size=128 mb'"));
    }

    @Test
    public void testGetDeploymentName() {
        assertThat(
                shuffleManagerParameters.getDeploymentName(), is(CLUSTER_ID + "-shufflemanager"));
    }

    @Test
    public void testGetReplicas() {
        assertThat(shuffleManagerParameters.getReplicas(), is(1));
    }
}
