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

package com.alibaba.flink.shuffle.e2e.flinkcluster;

import com.alibaba.flink.shuffle.e2e.TestJvmProcess;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleEnvironment;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToServiceAdapter;
import org.apache.flink.runtime.taskexecutor.TaskManagerRunner;
import org.apache.flink.runtime.util.ZooKeeperUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

/** A process of a task manager. */
public class TaskManagerProcess extends TestJvmProcess {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerProcess.class);

    private final String[] jvmArgs;

    private final int index;

    public TaskManagerProcess(String logDirName, Configuration config, int index) throws Exception {
        super("TaskExecutor-" + index, logDirName);
        Configuration configurationWithFallback =
                TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                        config, TaskManagerOptions.TOTAL_FLINK_MEMORY);

        final TaskExecutorProcessSpec processSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(configurationWithFallback);
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, processSpec.getManagedMemorySize());
        config.set(TaskManagerOptions.NETWORK_MEMORY_MIN, processSpec.getNetworkMemSize());
        config.set(TaskManagerOptions.NETWORK_MEMORY_MAX, processSpec.getNetworkMemSize());
        config.set(TaskManagerOptions.TASK_HEAP_MEMORY, processSpec.getTaskHeapSize());
        config.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, processSpec.getTaskOffHeapSize());
        config.set(
                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY,
                processSpec.getFlinkMemory().getFrameworkHeap());
        config.set(
                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY,
                processSpec.getFlinkMemory().getFrameworkOffHeap());
        config.set(TaskManagerOptions.JVM_OVERHEAD_MIN, processSpec.getJvmOverheadSize());
        config.set(TaskManagerOptions.JVM_OVERHEAD_MAX, processSpec.getJvmOverheadSize());

        ArrayList<String> args = new ArrayList<>();

        config.setInteger("taskmanager.index", index);
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            args.add("--" + entry.getKey());
            args.add(entry.getValue());
        }

        this.jvmArgs = new String[args.size()];
        args.toArray(jvmArgs);
        this.index = index;

        setJVMHeapMemory(processSpec.getJvmHeapMemorySize().getMebiBytes());
        setJvmDirectMemory(processSpec.getJvmDirectMemorySize().getMebiBytes());
        LOG.info(
                "JVM Process {} with process memory spec {}, heap memory {}, direct memory {}",
                getName(),
                processSpec,
                processSpec.getJvmHeapMemorySize().getMebiBytes(),
                processSpec.getJvmDirectMemorySize().getMebiBytes());
    }

    @Override
    public String[] getJvmArgs() {
        return jvmArgs;
    }

    @Override
    public String getEntryPointClassName() {
        return EntryPoint.class.getName();
    }

    /** Entry point for the Dispatcher process. */
    public static class EntryPoint {

        private static final Logger LOG = LoggerFactory.getLogger(EntryPoint.class);

        public static void main(String[] args) {
            try {
                final ParameterTool parameterTool = ParameterTool.fromArgs(args);
                Configuration cfg = parameterTool.getConfiguration();
                runTaskManager(cfg, PluginUtils.createPluginManagerFromRootFolder(cfg));
            } catch (Throwable t) {
                LOG.error("Failed to run the TaskManager process", t);
                System.exit(1);
            }
        }

        public static void runTaskManager(Configuration cfg, PluginManager pluginManager)
                throws Exception {
            TaskManagerRunner.TaskExecutorServiceFactory factory =
                    EntryPoint::createTaskExecutorService;
            TaskManagerRunner taskManagerRunner =
                    new TaskManagerRunner(cfg, pluginManager, factory);
            taskManagerRunner.start();
        }

        public static TaskManagerRunner.TaskExecutorService createTaskExecutorService(
                Configuration configuration,
                ResourceID resourceID,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                MetricRegistry metricRegistry,
                BlobCacheService blobCacheService,
                boolean localCommunicationOnly,
                ExternalResourceInfoProvider externalResourceInfoProvider,
                FatalErrorHandler fatalErrorHandler)
                throws Exception {

            final TaskExecutor taskExecutor =
                    TaskManagerRunner.startTaskManager(
                            configuration,
                            resourceID,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            metricRegistry,
                            blobCacheService,
                            localCommunicationOnly,
                            externalResourceInfoProvider,
                            fatalErrorHandler);
            startMetricsReportingDaemon(configuration, taskExecutor);
            return TaskExecutorToServiceAdapter.createFor(taskExecutor);
        }

        public static void startMetricsReportingDaemon(
                Configuration conf, TaskExecutor taskExecutor) throws Exception {
            Class<TaskExecutor> fooClass = (Class<TaskExecutor>) taskExecutor.getClass();
            Field field = fooClass.getDeclaredField("shuffleEnvironment");
            field.setAccessible(true);
            RemoteShuffleEnvironment env = (RemoteShuffleEnvironment) field.get(taskExecutor);
            NetworkBufferPool bufferPool = env.getNetworkBufferPool();
            CuratorFramework zkClient = ZooKeeperUtils.startCuratorFramework(conf);
            int index = conf.getInteger("taskmanager.index", -1);
            String zkPath = "/taskmanager-" + index;
            if (zkClient.checkExists().forPath(zkPath) != null) {
                zkClient.delete().forPath(zkPath);
            }
            zkClient.create().forPath(zkPath);
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    while (true) {
                                        int num = bufferPool.getNumberOfAvailableMemorySegments();
                                        TaskManagerStats tmStats = new TaskManagerStats(num);
                                        zkClient.setData().forPath(zkPath, tmStats.toBytes());
                                        Thread.sleep(500);
                                    }
                                } catch (Throwable t) {
                                    LOG.warn("Exception when reporting metrics.", t);
                                    System.exit(-1);
                                }
                            });
            thread.setDaemon(true);
            thread.start();
        }
    }

    static class TaskManagerStats {

        int availableBuffers;

        TaskManagerStats(int availableBuffers) {
            this.availableBuffers = availableBuffers;
        }

        byte[] toBytes() {
            ByteBuf buf = Unpooled.buffer();
            buf.writeInt(availableBuffers);
            return buf.array();
        }

        static TaskManagerStats fromBytes(byte[] bytes) {
            ByteBuffer wrapped = ByteBuffer.wrap(bytes);
            int numAvailableBuffers = wrapped.getInt();
            return new TaskManagerStats(numAvailableBuffers);
        }
    }
}
