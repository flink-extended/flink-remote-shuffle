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

package com.alibaba.flink.shuffle.kubernetes.operator.util;

/** Constants for kubernetes. */
public class Constants {

    public static final String API_VERSION = "v1";
    public static final String APPS_API_VERSION = "apps/v1";

    public static final String RESOURCE_NAME_CPU = "cpu";
    public static final String RESOURCE_NAME_MEMORY = "memory";
    public static final String RESOURCE_UNIT_MB = "Mi";

    public static final int MAXIMUM_CHARACTERS_OF_CLUSTER_ID = 45;

    // env
    public static final String ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS = "_POD_IP_ADDRESS";

    // labels
    public static final String LABEL_APPTYPE_KEY = "apptype";
    public static final String LABEL_APPTYPE_VALUE = "remoteshuffle";
    public static final String LABEL_APP_KEY = "app";
    public static final String LABEL_COMPONENT_KEY = "component";
    public static final String LABEL_COMPONENT_SHUFFLE_MANAGER = "shufflemanager";
    public static final String LABEL_COMPONENT_SHUFFLE_WORKER = "shuffleworker";

    public static final String ROLLING_UPDATE = "RollingUpdate";

    // volume
    public static final String VOLUME_NAME = "name";
    public static final String VOLUME_MOUNT_PATH = "mountPath";
    // empty dir
    public static final String EMPTY_DIR_VOLUME_MEDIUM = "medium";
    public static final String EMPTY_DIR_VOLUME_SIZE_LIMIT = "sizeLimit";
    // host path
    public static final String HOST_PATH_VOLUME_PATH = "path";
    public static final String HOST_PATH_VOLUME_TYPE = "type";

    // pod ip
    public static final String POD_IP_FIELD_PATH = "status.podIP";
    // pod name
    public static final String POD_NAME_FIELD_PATH = "metadata.name";
    public static final String ENV_REMOTE_SHUFFLE_POD_NAME = "_POD_NAME";

    // conf dir.
    public static final String REMOTE_SHUFFLE_CONF_VOLUME = "shuffle-config-file-volume";
    public static final String REMOTE_SHUFFLE_CONF_DIR = "/flink-remote-shuffle/conf";

    // user agent
    public static final String REMOTE_SHUFFLE_OPERATOR_USER_AGENT = "flink-remote-shuffle-operator";

    // Kubernetes start scripts
    public static final String SHUFFLE_MANAGER_SCRIPT_PATH =
            "/flink-remote-shuffle/bin/kubernetes-shufflemanager.sh";
    public static final String SHUFFLE_WORKER_SCRIPT_PATH =
            "/flink-remote-shuffle/bin/kubernetes-shuffleworker.sh";
}
