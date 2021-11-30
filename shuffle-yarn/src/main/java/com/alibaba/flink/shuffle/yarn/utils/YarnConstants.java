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

package com.alibaba.flink.shuffle.yarn.utils;

/** Constants for Yarn deployment. */
public class YarnConstants {

    public static final String MANAGER_APP_ENV_CLASS_PATH_KEY = "CLASSPATH";

    public static final String MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME = "shuffleManager";

    public static final String MANAGER_AM_TMP_PATH_NAME = "tmpDir";

    public static final String MANAGER_AM_JAR_FILE_PREFIX = "shuffle-dist";

    public static final String MANAGER_AM_JAR_FILE_EXCLUDE_SUFFIX = "tests.jar";

    public static final String MANAGER_AM_LOG4J_FILE_NAME = "log4j2.properties";

    public static final String MANAGER_AM_MAX_ATTEMPTS_KEY = "yarn.resourcemanager.am.max-attempts";

    /**
     * This parameter is the Id part of ApplicationId. This is only used to pass arguments from Yarn
     * client to application master.
     *
     * <p>It will connect with another parameter {@link #MANAGER_AM_APPID_TIMESTAMP_KEY} to form a
     * complete ApplicationId. Application master will get ip address from the ApplicationId and
     * store the address to Zookeeper.
     */
    public static final String MANAGER_AM_APPID_ID_KEY = "managerAppidId";

    /**
     * This parameter is the Timestamp part of ApplicationId. This is only used to pass arguments
     * from Yarn client to application master.
     *
     * <p>It will connect with another parameter {@link #MANAGER_AM_APPID_ID_KEY} to form a complete
     * ApplicationId. Application master will get ip address from the ApplicationId and store the
     * address to Zookeeper.
     */
    public static final String MANAGER_AM_APPID_TIMESTAMP_KEY = "managerAppidTimestamp";

    /**
     * Because only the Shuffle Manager is running in the application master and there are no other
     * containers, only one VCore is required.
     */
    public static final int MANAGER_AM_VCORE_COUNT = 1;

    /**
     * Define the name of shuffle worker service.
     *
     * <p>It's used to: 1. Configure shuffle service in NodeManger in yarn-site.xml. 2. Suggest the
     * auxiliary service name of shuffle worker in NodeManger.
     */
    public static final String WORKER_AUXILIARY_SERVICE_NAME =
            "yarn_remote_shuffle_worker_for_flink";

    // ------------------------------------------------------------------------
    //  Yarn Config Constants
    // ------------------------------------------------------------------------
    /**
     * Minimum valid size of memory in megabytes to start application master. If the specified
     * memory is less than this min value, the memory size will be replaced by the min value.
     *
     * <p>The values of configurations {@link #MANAGER_AM_MEMORY_SIZE_KEY} and {@link
     * #MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY} can not be less than this value.
     */
    public static final int MIN_VALID_AM_MEMORY_SIZE_MB = 128;

    /**
     * Local home directory containing all jars, configuration files and other resources, which is
     * used to start Shuffle Manager on Yarn.
     */
    public static final String MANAGER_HOME_DIR = "remote-shuffle.yarn.manager-home-dir";

    /** Application name when deploying Shuffle Manager service on Yarn. */
    public static final String MANAGER_APP_NAME_KEY = "remote-shuffle.yarn.manager-app-name";

    public static final String MANAGER_APP_NAME_DEFAULT = "Flink-Remote-Shuffle-Manager";

    /** Application priority when deploying Shuffle Manager service on Yarn. */
    public static final String MANAGER_APP_PRIORITY_KEY =
            "remote-shuffle.yarn.manager-app-priority";

    public static final int MANAGER_APP_PRIORITY_DEFAULT = 0;

    /** Application queue name when deploying Shuffle Manager service on Yarn. */
    public static final String MANAGER_APP_QUEUE_NAME_KEY =
            "remote-shuffle.yarn.manager-app-queue-name";

    public static final String MANAGER_APP_QUEUE_NAME_DEFAULT = "default";

    /**
     * Application master max attempt counts. The AM is a Shuffle Manager. In order to make Shuffle
     * Manager run more stably, the attempt count is set as a very large value.
     */
    public static final String MANAGER_AM_MAX_ATTEMPTS_VAL_KEY =
            "remote-shuffle.yarn.manager-am-max-attempts";

    public static final int MANAGER_AM_MAX_ATTEMPTS_VAL_DEFAULT = 1000000;

    /**
     * Size of memory in megabytes allocated for starting application master, which is used to
     * specify memory size for Shuffle Manager. If the configured value is smaller than
     * MIN_VALID_AM_MEMORY_SIZE_MB, the memory size will be replaced by MIN_VALID_AM_MEMORY_SIZE_MB.
     */
    public static final String MANAGER_AM_MEMORY_SIZE_KEY =
            "remote-shuffle.yarn.manager-am-memory-size-mb";

    public static final int MANAGER_AM_MEMORY_SIZE_DEFAULT = 2048;

    /**
     * Size of overhead memory in megabytes allocated for starting application master, which is used
     * to specify overhead memory size for ShuffleManager. If the configured value is smaller than
     * MIN_VALID_AM_MEMORY_SIZE_MB, the overhead memory size will be replaced by
     * MIN_VALID_AM_MEMORY_SIZE_MB.
     */
    public static final String MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY =
            "remote-shuffle.yarn.manager-am-memory-overhead-mb";

    public static final int MANAGER_AM_MEMORY_OVERHEAD_SIZE_DEFAULT = 512;

    /**
     * Use this option if you want to modify other JVM options for the ShuffleManager running in the
     * application master. For example, you can configure JVM heap size, JVM GC logs, JVM GC
     * operations, etc.
     */
    public static final String MANAGER_AM_MEMORY_JVM_OPTIONS_KEY =
            "remote-shuffle.yarn.manager-am-jvm-options";

    public static final String MANAGER_AM_MEMORY_JVM_OPTIONS_DEFAULT = "";

    /**
     * Shuffle Manager is started in a container, the container will keep sending heartbeats to Yarn
     * resource manager, and this parameter indicates the heartbeat interval.
     */
    public static final String MANAGER_RM_HEARTBEAT_INTERVAL_MS_KEY =
            "remote-shuffle.yarn.manager-rm-heartbeat-interval-ms";

    public static final int MANAGER_RM_HEARTBEAT_INTERVAL_MS_DEFAULT = 1000;
}
