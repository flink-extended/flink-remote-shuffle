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

package com.alibaba.flink.shuffle.e2e.shufflecluster;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.NetUtils;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManager;
import com.alibaba.flink.shuffle.coordinator.manager.entrypoint.ShuffleManagerEntrypoint;
import com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils;
import com.alibaba.flink.shuffle.coordinator.utils.EnvironmentInformation;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.RestOptions;
import com.alibaba.flink.shuffle.core.config.memory.ShuffleManagerProcessSpec;
import com.alibaba.flink.shuffle.e2e.TestJvmProcess;

import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link ShuffleManager} instance running in a separate JVM. */
public class ShuffleManagerProcess extends TestJvmProcess {

    private final String[] jvmArgs;

    public ShuffleManagerProcess(String logDirName, Configuration configuration) throws Exception {
        super("ShuffleManager", logDirName);
        this.jvmArgs = LocalShuffleClusterUtils.generateDynamicConfigs(configuration);

        configuration.setInteger(ManagerOptions.RPC_PORT, NetUtils.getAvailablePort());
        configuration.setInteger(RestOptions.REST_MANAGER_BIND_PORT, NetUtils.getAvailablePort());
        ShuffleManagerProcessSpec processSpec = new ShuffleManagerProcessSpec(configuration);
        setJvmDirectMemory(processSpec.getJvmDirectMemorySize().getMebiBytes());
        setJVMHeapMemory(processSpec.getJvmHeapMemorySize().getMebiBytes());
    }

    @Override
    public String[] getJvmArgs() {
        return jvmArgs;
    }

    @Override
    public String getEntryPointClassName() {
        return EntryPoint.class.getName();
    }

    /** Entry point for the ShuffleManager process. */
    public static class EntryPoint {

        private static final Logger LOG = LoggerFactory.getLogger(EntryPoint.class);

        public static void main(String[] args) throws Exception {
            // startup checks and logging
            EnvironmentInformation.logEnvironmentInfo(LOG, "Shuffle Manager", args);
            SignalHandler.register(LOG);
            JvmShutdownSafeguard.installAsShutdownHook(LOG);

            Configuration configuration = ClusterEntrypointUtils.parseParametersOrExit(args);

            configuration.setString(ManagerOptions.RPC_ADDRESS, "127.0.0.1");
            ShuffleManagerEntrypoint shuffleManagerEntrypoint =
                    new ShuffleManagerEntrypoint(configuration);

            ShuffleManagerEntrypoint.runShuffleManagerEntrypoint(shuffleManagerEntrypoint);
        }
    }
}
