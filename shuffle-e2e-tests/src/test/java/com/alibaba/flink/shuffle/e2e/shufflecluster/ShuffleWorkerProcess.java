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
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorker;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner;
import com.alibaba.flink.shuffle.core.config.RestOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.config.memory.ShuffleWorkerProcessSpec;
import com.alibaba.flink.shuffle.e2e.TestJvmProcess;

/** A {@link ShuffleWorker} instance running in a separate JVM. */
public class ShuffleWorkerProcess extends TestJvmProcess {

    private final String[] jvmArgs;

    private final int dataPort;

    private final String dataDir;

    public ShuffleWorkerProcess(
            String logDirName, Configuration configuration, int index, int dataPort, String dataDir)
            throws Exception {
        super("ShuffleWorker-" + index, logDirName);
        this.dataPort = dataPort;
        this.dataDir = dataDir;
        configuration.setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
        configuration.setString(StorageOptions.STORAGE_LOCAL_DATA_DIRS, dataDir);
        configuration.setInteger(RestOptions.REST_WORKER_BIND_PORT, NetUtils.getAvailablePort());
        this.jvmArgs = LocalShuffleClusterUtils.generateDynamicConfigs(configuration);

        ShuffleWorkerProcessSpec processSpec = new ShuffleWorkerProcessSpec(configuration);
        setJvmDirectMemory(processSpec.getJvmDirectMemorySize().getMebiBytes());
        setJVMHeapMemory(processSpec.getJvmHeapMemorySize().getMebiBytes());
    }

    @Override
    public String[] getJvmArgs() {
        return jvmArgs;
    }

    @Override
    public String getEntryPointClassName() {
        return ShuffleWorkerRunner.class.getName();
    }

    public int getDataPort() {
        return dataPort;
    }

    public String getDataDir() {
        return dataDir;
    }
}
