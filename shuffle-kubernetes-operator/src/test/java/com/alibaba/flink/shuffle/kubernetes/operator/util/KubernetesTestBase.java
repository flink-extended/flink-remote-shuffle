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

import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ConfigMapVolume;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Base class for kubernetes unit test. */
public class KubernetesTestBase {

    // common config.
    protected static final String CLUSTER_ID = "TestingCluster";
    protected static final String NAMESPACE = "default";
    protected static final String SERVICE_ACCOUNT = "default";

    // container image
    protected static final String CONTAINER_NAME = "TestingContainer";
    protected static final String CONTAINER_IMAGE = "flink-remote-shuffle-k8s-test:latest";

    // container cpu and memory
    protected static final double CONTAINER_CPU = 1.5;
    protected static final int CONTAINER_FRAMEWORK_HEAP_MEMORY_MB = 256;
    protected static final int CONTAINER_FRAMEWORK_OFF_HEAP_MEMORY_MB = 128;
    protected static final MemorySize NETWORK_MEMORY_BUFFER_SIZE = MemorySize.parse("1m");
    protected static final MemorySize NETWORK_WRITING_MEMORY_SIZE = MemorySize.parse("32m");
    protected static final MemorySize NETWORK_READING_MEMORY_SIZE = MemorySize.parse("32m");

    protected static final int CONTAINER_JVM_METASPACE_MB = 32;
    protected static final int CONTAINER_JVM_OVERHEAD_MB = 32;

    // container command
    protected static final String CONTAINER_JVM_OPTIONS =
            "-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4";

    // deployment, daemonset, configmap name
    protected static final String DEPLOYMENT_NAME = "TestingDeployment";
    protected static final String DAEMON_SET_NAME = "TestingDaemonSet";
    protected static final String CONFIG_MAP_NAME = "TestingConfigMap";

    protected static final List<Map<String, String>> CONTAINER_VOLUME_MOUNTS =
            new ArrayList<Map<String, String>>() {
                {
                    add(
                            new HashMap<String, String>() {
                                {
                                    put(Constants.VOLUME_NAME, "disk1");
                                    put(Constants.VOLUME_MOUNT_PATH, "/opt/disk1");
                                }
                            });
                    add(
                            new HashMap<String, String>() {
                                {
                                    put(Constants.VOLUME_NAME, "disk2");
                                    put(Constants.VOLUME_MOUNT_PATH, "/opt/disk2");
                                }
                            });
                }
            };

    protected static final List<Map<String, String>> EMPTY_DIR_VOLUMES =
            new ArrayList<Map<String, String>>() {
                {
                    add(
                            new HashMap<String, String>() {
                                {
                                    put(Constants.VOLUME_NAME, "disk1");
                                    put(Constants.EMPTY_DIR_VOLUME_SIZE_LIMIT, "5Gi");
                                    put(Constants.VOLUME_MOUNT_PATH, "/opt/disk1");
                                }
                            });
                    add(
                            new HashMap<String, String>() {
                                {
                                    put(Constants.VOLUME_NAME, "disk2");
                                    put(Constants.EMPTY_DIR_VOLUME_SIZE_LIMIT, "10Gi");
                                    put(Constants.VOLUME_MOUNT_PATH, "/opt/disk2");
                                }
                            });
                }
            };

    protected static final List<Map<String, String>> HOST_PATH_VOLUMES =
            new ArrayList<Map<String, String>>() {
                {
                    add(
                            new HashMap<String, String>() {
                                {
                                    put(Constants.VOLUME_NAME, "disk1");
                                    put(Constants.HOST_PATH_VOLUME_PATH, "/dump/1");
                                    put(Constants.VOLUME_MOUNT_PATH, "/opt/disk1");
                                }
                            });
                    add(
                            new HashMap<String, String>() {
                                {
                                    put(Constants.VOLUME_NAME, "disk2");
                                    put(Constants.HOST_PATH_VOLUME_PATH, "/dump/2");
                                    put(Constants.VOLUME_MOUNT_PATH, "/opt/disk2");
                                }
                            });
                }
            };

    protected static final ConfigMapVolume CONFIG_MAP_VOLUME =
            new ConfigMapVolume(
                    "disk1",
                    "config-map",
                    new HashMap<String, String>() {
                        {
                            put("log4j2.xml", "log4j2.xml");
                            put("log4j.properties", "log4j.properties");
                        }
                    },
                    "/opt/conf");

    protected static final List<Map<String, String>> TOLERATIONS =
            new ArrayList<Map<String, String>>() {
                {
                    add(
                            new HashMap<String, String>() {
                                {
                                    put("key", "key1");
                                    put("operator", "Equal");
                                    put("value", "value1");
                                    put("effect", "NoSchedule");
                                }
                            });
                }
            };

    protected static final Map<String, String> USER_LABELS =
            new HashMap<String, String>() {
                {
                    put("label1", "value1");
                    put("label2", "value2");
                }
            };

    protected static final Map<String, String> NODE_SELECTOR =
            new HashMap<String, String>() {
                {
                    put("env", "production");
                    put("disk", "ssd");
                }
            };

    protected static final Map<String, String> RESOURCE_LIMIT_FACTOR =
            new HashMap<String, String>() {
                {
                    put("cpu", "3.0");
                    put("memory", "1.5");
                }
            };

    protected static int getTotalMemory(boolean containsNetwork) {
        int totalMemory =
                CONTAINER_FRAMEWORK_HEAP_MEMORY_MB
                        + CONTAINER_FRAMEWORK_OFF_HEAP_MEMORY_MB
                        + CONTAINER_JVM_METASPACE_MB
                        + CONTAINER_JVM_OVERHEAD_MB;
        if (containsNetwork) {
            totalMemory +=
                    NETWORK_READING_MEMORY_SIZE.getMebiBytes()
                            + NETWORK_WRITING_MEMORY_SIZE.getMebiBytes();
        }
        return totalMemory;
    }

    protected static Map<String, String> getCommonLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put(Constants.LABEL_APPTYPE_KEY, Constants.LABEL_APPTYPE_VALUE);
        labels.put(Constants.LABEL_APP_KEY, CLUSTER_ID);
        return labels;
    }
}
