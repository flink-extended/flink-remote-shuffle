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

package com.alibaba.flink.shuffle.kubernetes.manager;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.JvmShutdownSafeguard;
import com.alibaba.flink.shuffle.common.utils.SignalHandler;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaMode;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManager;
import com.alibaba.flink.shuffle.coordinator.manager.entrypoint.ShuffleManagerEntrypoint;
import com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils;
import com.alibaba.flink.shuffle.coordinator.utils.EnvironmentInformation;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils.STARTUP_FAILURE_RETURN_CODE;

/** The kubernetes entrypoint class for the {@link ShuffleManager}. */
public class KubernetesShuffleManagerRunner {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesShuffleManagerRunner.class);

    public static final String ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS = "_POD_IP_ADDRESS";

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "Shuffle Manager", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if (maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        try {
            Configuration configuration = ClusterEntrypointUtils.parseParametersOrExit(args);
            ShuffleManagerEntrypoint shuffleManagerEntrypoint =
                    new ShuffleManagerEntrypoint(loadConfiguration(configuration));
            ShuffleManagerEntrypoint.runShuffleManagerEntrypoint(shuffleManagerEntrypoint);
        } catch (Throwable t) {
            LOG.error("ShuffleManager initialization failed.", t);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }
    }

    /**
     * For HA cluster, {@link ManagerOptions#RPC_ADDRESS} will be set to the pod ip address. The
     * ShuffleWorker use Zookeeper or other high-availability service to find the address of
     * ShuffleManager.
     *
     * @return Updated configuration
     */
    static Configuration loadConfiguration(Configuration conf) {
        Configuration configuration = new Configuration(conf);
        if (HaMode.isHighAvailabilityModeActivated(configuration)) {
            final String ipAddress = System.getenv(ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS);
            checkState(
                    ipAddress != null,
                    "ShuffleManager ip address environment variable "
                            + ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS
                            + " not set");
            configuration.setString(ManagerOptions.RPC_ADDRESS, ipAddress);
        }

        return configuration;
    }
}
