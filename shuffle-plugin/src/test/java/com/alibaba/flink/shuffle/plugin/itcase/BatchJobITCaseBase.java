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

package com.alibaba.flink.shuffle.plugin.itcase;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.NetUtils;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.minicluster.ShuffleMiniCluster;
import com.alibaba.flink.shuffle.minicluster.ShuffleMiniClusterConfiguration;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;
import org.apache.flink.util.ExceptionUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.function.Supplier;

/** A base class for batch job cases which using the remote shuffle. */
public abstract class BatchJobITCaseBase {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected int numShuffleWorkers = 4;

    protected int numTaskManagers = 4;

    protected int numSlotsPerTaskManager = 4;

    protected final Configuration configuration = new Configuration();

    protected final org.apache.flink.configuration.Configuration flinkConfiguration =
            new org.apache.flink.configuration.Configuration();

    protected MiniCluster flinkCluster;

    protected ShuffleMiniCluster shuffleCluster;

    protected Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier = null;

    @Before
    public void before() throws Exception {
        // basic configuration
        int shuffleManagerRpcPort = NetUtils.getAvailablePort();
        String address = InetAddress.getLocalHost().getHostAddress();
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS,
                temporaryFolder.getRoot().getAbsolutePath());
        configuration.setString(ManagerOptions.RPC_ADDRESS, address);
        configuration.setString(ManagerOptions.RPC_BIND_ADDRESS, address);
        configuration.setString(WorkerOptions.BIND_HOST, address);
        configuration.setString(WorkerOptions.HOST, address);
        configuration.setInteger(ManagerOptions.RPC_PORT, shuffleManagerRpcPort);
        configuration.setString(StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES.key(), "0b");

        // flink basic configuration.
        int jobManagerRpcPort = NetUtils.getAvailablePort();
        flinkConfiguration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConfiguration.setString(
                ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS,
                RemoteShuffleServiceFactory.class.getName());
        flinkConfiguration.setString(ManagerOptions.RPC_ADDRESS.key(), address);
        flinkConfiguration.setInteger(JobManagerOptions.PORT, jobManagerRpcPort);
        flinkConfiguration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);
        flinkConfiguration.setString(RestOptions.BIND_PORT, "0");
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        flinkConfiguration.set(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.4F);
        flinkConfiguration.setString(PluginOptions.MEMORY_PER_INPUT_GATE.key(), "8m");
        flinkConfiguration.setString(PluginOptions.MEMORY_PER_RESULT_PARTITION.key(), "8m");
        flinkConfiguration.setInteger(ManagerOptions.RPC_PORT.key(), shuffleManagerRpcPort);

        // setup special config.
        setup();

        ShuffleMiniClusterConfiguration clusterConf =
                new ShuffleMiniClusterConfiguration.Builder()
                        .setConfiguration(configuration)
                        .setNumShuffleWorkers(numShuffleWorkers)
                        .setCommonBindAddress(address)
                        .build();
        shuffleCluster = new ShuffleMiniCluster(clusterConf);
        shuffleCluster.start();

        TestingMiniClusterConfiguration miniClusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(flinkConfiguration)
                        .setNumTaskManagers(numTaskManagers)
                        .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                        .build();

        flinkCluster =
                TestingMiniCluster.newBuilder(miniClusterConfiguration)
                        .setHighAvailabilityServicesSupplier(highAvailabilityServicesSupplier)
                        .build();
        flinkCluster.start();
    }

    @After
    public void after() {
        Throwable exception = null;

        try {
            if (flinkCluster != null) {
                flinkCluster.close();
            }
        } catch (Throwable throwable) {
            exception = throwable;
        }

        try {
            if (shuffleCluster != null) {
                shuffleCluster.close();
            }
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
        }

        if (exception != null) {
            ExceptionUtils.rethrow(exception);
        }
    }

    abstract void setup();
}
