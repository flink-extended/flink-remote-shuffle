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

package com.alibaba.flink.shuffle.yarn;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
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
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.yarn.utils.TestTimeoutUtils.waitAllCompleted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** A base class for batch job cases which using the remote shuffle. */
public abstract class BatchJobTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(BatchJobTestBase.class);

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected int numShuffleWorkers = 4;

    protected int numTaskManagers = 2;

    protected int numSlotsPerTaskManager = 2;

    protected final Configuration configuration = new Configuration();

    protected final org.apache.flink.configuration.Configuration flinkConfiguration =
            new org.apache.flink.configuration.Configuration();

    protected MiniCluster flinkCluster;

    protected ShuffleMiniCluster shuffleCluster;

    protected Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier = null;

    @Before
    public void before() throws Exception {
        // basic configuration
        String address = InetAddress.getLocalHost().getHostAddress();
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS,
                temporaryFolder.getRoot().getAbsolutePath());
        configuration.setString(ManagerOptions.RPC_ADDRESS, address);
        configuration.setString(ManagerOptions.RPC_BIND_ADDRESS, address);
        configuration.setString(WorkerOptions.BIND_HOST, address);
        configuration.setString(WorkerOptions.HOST, address);
        configuration.setInteger(ManagerOptions.RPC_PORT, ManagerOptions.RPC_PORT.defaultValue());
        configuration.setInteger(
                ManagerOptions.RPC_BIND_PORT, ManagerOptions.RPC_PORT.defaultValue());
        configuration.setDuration(ClusterOptions.REGISTRATION_TIMEOUT, Duration.ofHours(1));

        // flink basic configuration.
        flinkConfiguration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConfiguration.setString(
                ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS,
                RemoteShuffleServiceFactory.class.getName());
        flinkConfiguration.setString(ManagerOptions.RPC_ADDRESS.key(), address);
        flinkConfiguration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 5000L);
        flinkConfiguration.setString(RestOptions.BIND_PORT, "0");
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        flinkConfiguration.set(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.4F);
        flinkConfiguration.setString(PluginOptions.MEMORY_PER_INPUT_GATE.key(), "8m");
        flinkConfiguration.setString(PluginOptions.MEMORY_PER_RESULT_PARTITION.key(), "8m");
        flinkConfiguration.setString(
                NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX, "512mb");

        // setup special config.
        setup();

        asyncStartShuffleAndFlinkCluster(address);
    }

    private void startShuffleAndFlinkCluster(String address) throws Exception {
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
                new TestingMiniCluster(miniClusterConfiguration, highAvailabilityServicesSupplier);
        flinkCluster.start();
    }

    private void asyncStartShuffleAndFlinkCluster(String address) throws Exception {
        long start = System.currentTimeMillis();
        CompletableFuture<Boolean> startClusterFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                startShuffleAndFlinkCluster(address);
                                return true;
                            } catch (Exception e) {
                                LOG.error("Failed to setup shuffle and flink cluster, ", e);
                                return false;
                            }
                        });
        List<Boolean> results =
                waitAllCompleted(
                        Collections.singletonList(startClusterFuture), 600, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - start;
        LOG.info("The process of start shuffle and flink cluster took " + duration + " ms");
        assertEquals(1, results.size());
        assertTrue(results.get(0));
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

        shutdown();
    }

    abstract void setup() throws Exception;

    abstract void shutdown();
}
