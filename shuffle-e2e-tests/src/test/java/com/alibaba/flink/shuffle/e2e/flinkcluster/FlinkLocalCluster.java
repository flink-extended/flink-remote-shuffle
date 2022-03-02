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

import com.alibaba.flink.shuffle.core.config.HeartbeatOptions;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.e2e.TestingListener;
import com.alibaba.flink.shuffle.e2e.utils.LogErrorHandler;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestUtils;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.shuffle.ShuffleServiceOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.curator.framework.CuratorFramework;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A testing Flink local cluster, which contains a dispatcher process and several task manager
 * processes.
 */
public class FlinkLocalCluster {

    public final TemporaryFolder temporaryFolder;

    private final String logDirName;

    private Configuration conf;

    private final int numTaskManagers;

    private final TaskManagerProcess[] taskManagers;

    private CuratorFramework zkClient;

    private final String zkConnect;

    private final File zkPath;

    private DispatcherProcess dispatcher;

    private DispatcherGateway dispatcherGateway;

    private HighAvailabilityServices highAvailabilityServices;

    private LeaderRetrievalService leaderRetrievalService;

    private RpcService rpcService;

    public FlinkLocalCluster(
            String logDirName,
            int numTaskManagers,
            TemporaryFolder temporaryFolder,
            String zkConnect,
            Configuration conf)
            throws Exception {
        this.logDirName = logDirName;
        this.numTaskManagers = numTaskManagers;
        this.taskManagers = new TaskManagerProcess[numTaskManagers];
        this.temporaryFolder = temporaryFolder;
        this.zkConnect = zkConnect;
        this.zkPath = temporaryFolder.newFolder();
        initConfig(conf);
    }

    public void start() throws Exception {
        this.dispatcher = new DispatcherProcess(logDirName, conf);
        this.dispatcher.startProcess();
        this.highAvailabilityServices =
                HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
                        conf, Executors.newSingleThreadExecutor(), LogErrorHandler.INSTANCE);
        RpcSystem rpcSystem = RpcSystem.load(conf);
        rpcService = rpcSystem.remoteServiceBuilder(conf, "localhost", "0").createAndStart();
        for (int i = 0; i < numTaskManagers; i++) {
            taskManagers[i] = new TaskManagerProcess(logDirName, conf, i);
            taskManagers[i].startProcess();
        }

        Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));
        Pair<String, DispatcherId> pair = waitForDispatcher(deadline);
        waitForTaskManagers(numTaskManagers, pair.getLeft(), pair.getRight(), deadline.timeLeft());
    }

    private Pair<String, DispatcherId> waitForDispatcher(Deadline deadline) throws Exception {
        TestingListener listener = new TestingListener();
        leaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();
        leaderRetrievalService.start(listener);

        listener.waitForNewLeader(deadline.timeLeft().toMillis());
        return Pair.of(listener.getAddress(), DispatcherId.fromUuid(listener.getLeaderSessionID()));
    }

    public void shutdown() throws Exception {
        for (int i = 0; i < numTaskManagers; i++) {
            taskManagers[i].destroy();
        }

        dispatcher.destroy();

        leaderRetrievalService.stop();

        RpcUtils.terminateRpcService(rpcService, Time.seconds(30L));

        highAvailabilityServices.closeAndCleanupAllData();

        ZooKeeperTestUtils.deleteAll(zkClient);
        zkClient.close();
    }

    public Configuration getConfig() {
        return conf;
    }

    public CuratorFramework getZKClient() {
        return checkNotNull(zkClient);
    }

    public String getZKConnect() {
        return zkConnect;
    }

    public String getZKPath() {
        return zkPath.getPath();
    }

    private void initConfig(Configuration c) throws Exception {
        conf = ZooKeeperTestUtils.configureZooKeeperHAForFlink(c, zkConnect, zkPath.getPath());
        conf.set(AkkaOptions.ASK_TIMEOUT, "600 s");
        conf.set(WebOptions.TIMEOUT, 600000L);
        conf.set(JobManagerOptions.ADDRESS, "localhost");
        conf.set(RestOptions.BIND_PORT, "0");
        conf.set(HeartbeatManagerOptions.HEARTBEAT_INTERVAL, 1000L);
        conf.set(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 20000L);
        conf.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, 1);
        conf.set(ExecutionConfigOptions.TABLE_EXEC_SHUFFLE_MODE, "ALL_EDGES_BLOCKING");
        conf.set(TaskManagerOptions.CPU_CORES, 1.0);
        String cpPath = temporaryFolder.newFolder().getAbsoluteFile().toURI().toString();
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, cpPath);
        conf.setInteger(TransferOptions.CLIENT_CONNECT_TIMEOUT.key(), 3);
        conf.setString(
                com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions.HA_MODE.key(),
                "ZOOKEEPER");
        conf.setString(
                com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM
                        .key(),
                zkConnect);
        conf.setLong(HeartbeatOptions.HEARTBEAT_JOB_INTERVAL.key(), 3000);
        String shuffleServiceClassName = RemoteShuffleServiceFactory.class.getName();
        conf.set(ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS, shuffleServiceClassName);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 100);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        conf.setString(WorkerOptions.MAX_WORKER_RECOVER_TIME.key(), "60s");

        // JM memory config
        conf.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(2048));
        conf.set(JobManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(32));
        conf.set(JobManagerOptions.JVM_OVERHEAD_MIN, MemorySize.ofMebiBytes(32));
        conf.set(JobManagerOptions.JVM_HEAP_MEMORY, MemorySize.ofMebiBytes(1536));

        // TM memory config
        conf.setString(PluginOptions.MEMORY_PER_INPUT_GATE.key(), "16m");
        conf.setString(PluginOptions.MEMORY_PER_RESULT_PARTITION.key(), "8m");
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(1024));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(64));
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(64));
        conf.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ofMebiBytes(32));
        conf.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.ofMebiBytes(32));
        conf.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.ofMebiBytes(32));

        // ZK config
        zkClient = ZooKeeperTestUtils.createZKClientForFlink(conf);
    }

    private void waitForTaskManagers(
            int numTaskManagers, String addr, DispatcherId dispatcherId, Duration timeLeft)
            throws ExecutionException, InterruptedException {
        dispatcherGateway = rpcService.connect(addr, dispatcherId, DispatcherGateway.class).get();
        FutureUtils.retrySuccessfulWithDelay(
                        () ->
                                dispatcherGateway.requestClusterOverview(
                                        Time.milliseconds(timeLeft.toMillis())),
                        Time.milliseconds(50L),
                        org.apache.flink.api.common.time.Deadline.fromNow(
                                Duration.ofMillis(timeLeft.toMillis())),
                        overview -> overview.getNumTaskManagersConnected() >= numTaskManagers,
                        new ScheduledExecutorServiceAdapter(
                                Executors.newSingleThreadScheduledExecutor()))
                .get();
    }

    public int getNumTaskManagersConnected() throws ExecutionException, InterruptedException {
        ClusterOverview view = dispatcherGateway.requestClusterOverview(Time.seconds(10L)).get();
        return view.getNumTaskManagersConnected();
    }

    public void killTaskManager(int index) throws Exception {
        Process kill = Runtime.getRuntime().exec("kill -9 " + taskManagers[index].getProcessId());
        kill.waitFor();
    }

    public boolean isTaskManagerAlive(int index) {
        return taskManagers[index].isAlive();
    }

    public void recoverTaskManager(int index) throws Exception {
        checkState(!isTaskManagerAlive(index));
        taskManagers[index] = new TaskManagerProcess(logDirName, conf, index);
        taskManagers[index].startProcess();
    }

    public void cancelJobs() throws Exception {
        Time timeout = Time.seconds(10);
        Collection<JobID> jobIDs = dispatcherGateway.listJobs(timeout).get();
        jobIDs.forEach(jobID -> dispatcherGateway.cancelJob(jobID, timeout));
    }

    public void checkResourceReleased() throws Exception {
        Thread.sleep(3000); // Wait a moment allowing TM to report stats with delay.
        for (int i = 0; i < taskManagers.length; ++i) {
            if (!taskManagers[i].isAlive()) {
                continue;
            }
            byte[] bytes = zkClient.getData().forPath("/taskmanager-" + i);
            TaskManagerProcess.TaskManagerStats stats =
                    TaskManagerProcess.TaskManagerStats.fromBytes(bytes);
            int actual = stats.availableBuffers;
            int expect = 2048; // 128M(network memory size) / 32k(page size)
            String msg = String.format("TM memory leak, expect=%d, actual=%d", expect, actual);
            if (actual != expect) {
                throw new AssertionError(msg);
            }
        }
    }

    public void printProcessLog() {
        dispatcher.printProcessLog();
        for (TaskManagerProcess worker : taskManagers) {
            worker.printProcessLog();
        }
    }
}
