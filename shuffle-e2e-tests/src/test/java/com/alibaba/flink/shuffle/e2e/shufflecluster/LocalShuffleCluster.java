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

package com.alibaba.flink.shuffle.e2e.shufflecluster;

import com.alibaba.flink.shuffle.client.ShuffleManagerClient;
import com.alibaba.flink.shuffle.client.ShuffleManagerClientConfiguration;
import com.alibaba.flink.shuffle.client.ShuffleManagerClientImpl;
import com.alibaba.flink.shuffle.client.ShuffleWorkerStatusListener;
import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.functions.RunnableWithException;
import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.common.utils.NetUtils;
import com.alibaba.flink.shuffle.coordinator.heartbeat.HeartbeatServicesUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServiceUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PartitionPlacementStrategyLoader;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetricKeys;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerMetrics;
import com.alibaba.flink.shuffle.core.config.HeartbeatOptions;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.e2e.TestJvmProcess;
import com.alibaba.flink.shuffle.e2e.utils.CommonTestUtils;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestUtils;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;
import com.alibaba.flink.shuffle.rpc.utils.RpcUtils;

import org.apache.flink.api.common.time.Deadline;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * A testing local shuffle cluster, which contains a shuffle manager process and several shuffle
 * worker processes.
 */
public class LocalShuffleCluster {

    private static final Logger LOG = LoggerFactory.getLogger(LocalShuffleCluster.class);

    private final String logDirName;

    private final Configuration config;

    private final MemorySize bufferSize;

    private final int numWorkers;

    private final Path parentDataDir;

    private final String zkConnect;

    public ShuffleManagerProcess shuffleManager;

    public ShuffleWorkerProcess[] shuffleWorkers;

    public ShuffleManagerClient shuffleManagerClient;

    private CuratorFramework zkClient;

    private RemoteShuffleRpcService rpcService;

    public LocalShuffleCluster(
            String logDirName,
            int numWorkers,
            String zkConnect,
            Path parentDataDir,
            Configuration otherConfigurations) {
        this.logDirName = logDirName;
        this.numWorkers = numWorkers;
        this.shuffleWorkers = new ShuffleWorkerProcess[numWorkers];
        this.zkConnect = zkConnect;
        this.parentDataDir = parentDataDir;
        this.config = new Configuration(otherConfigurations);

        this.bufferSize = config.getMemorySize(MemoryOptions.MEMORY_BUFFER_SIZE);
        config.setMemorySize(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_READING, MemoryOptions.MIN_VALID_MEMORY_SIZE);
        config.setMemorySize(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING, MemoryOptions.MIN_VALID_MEMORY_SIZE);
        config.setString(
                ManagerOptions.PARTITION_PLACEMENT_STRATEGY,
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME);
        config.setMemorySize(StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, MemorySize.ZERO);
    }

    public void start() throws Exception {
        LOG.info(
                "Starting the local shuffle cluster with parent data directory {}.", parentDataDir);
        initConfig();
        shuffleManager = new ShuffleManagerProcess(logDirName, config);
        shuffleManager.startProcess();

        for (int i = 0; i < numWorkers; i++) {
            String workerDataDir = "ShuffleWorker-" + i;
            Files.createDirectory(parentDataDir.resolve(workerDataDir));
            shuffleWorkers[i] =
                    new ShuffleWorkerProcess(
                            logDirName,
                            new Configuration(config),
                            i,
                            NetUtils.getAvailablePort(),
                            getDataDirForWorker(i));
            shuffleWorkers[i].startProcess();
        }

        rpcService =
                AkkaRpcServiceUtils.remoteServiceBuilder(config, "localhost", "0").createAndStart();

        HaServices haServices = HaServiceUtils.createHAServices(config);

        shuffleManagerClient =
                new ShuffleManagerClientImpl(
                        RandomIDUtils.randomJobId(),
                        new ShuffleWorkerStatusListener() {
                            @Override
                            public void notifyIrrelevantWorker(InstanceID workerID) {}

                            @Override
                            public void notifyRelevantWorker(
                                    InstanceID workerID,
                                    Set<DataPartitionCoordinate> dataPartitions) {}
                        },
                        rpcService,
                        (Throwable throwable) -> new ShutDownFatalErrorHandler(),
                        ShuffleManagerClientConfiguration.fromConfiguration(config),
                        haServices,
                        HeartbeatServicesUtils.createManagerJobHeartbeatServices(config));
        shuffleManagerClient.start();

        waitForShuffleClusterReady(Duration.ofMinutes(5));

        LOG.info("Local shuffle cluster started successfully.");
    }

    public int getNumRegisteredWorkers() throws Exception {
        return shuffleManagerClient.getNumberOfRegisteredWorkers().get();
    }

    public String getDataDirForWorker(int index) {
        return new File(parentDataDir.toFile(), "ShuffleWorker-" + index).getAbsolutePath();
    }

    public void printProcessLog() {
        shuffleManager.printProcessLog();
        for (TestJvmProcess worker : shuffleWorkers) {
            worker.printProcessLog();
        }
    }

    public void shutdown() throws Exception {
        for (int i = 0; i < numWorkers; i++) {
            try {
                shuffleWorkers[i].destroy();
            } catch (Throwable throwable) {
                LOG.error("Failed to stop shuffle worker.", throwable);
            }
        }

        try {
            shuffleManager.destroy();
        } catch (Throwable throwable) {
            LOG.error("Failed to stop shuffle manager.", throwable);
        }

        try {
            RpcUtils.terminateRpcService(rpcService, 30000L);
        } catch (Throwable throwable) {
            LOG.error("Failed to stop rpc service.", throwable);
        }

        try {
            ZooKeeperTestUtils.deleteAll(zkClient);
        } catch (Throwable throwable) {
            LOG.error("Failed to clear zk data.", throwable);
        }

        try {
            zkClient.close();
        } catch (Throwable throwable) {
            LOG.error("Failed to stop zk client.", throwable);
        }
    }

    public Optional<Integer> findShuffleWorker(int processId) {
        for (int i = 0; i < shuffleWorkers.length; ++i) {
            if (shuffleWorkers[i].getProcessId() == processId) {
                return Optional.of(i);
            }
        }

        return Optional.empty();
    }

    public void killShuffleWorker(int index) {
        shuffleWorkers[index].destroy();
    }

    public void killShuffleWorkerForcibly(int index) throws Exception {
        Process kill = Runtime.getRuntime().exec("kill -9 " + shuffleWorkers[index].getProcessId());
        kill.waitFor();
    }

    public boolean isShuffleWorkerAlive(int index) {
        return shuffleWorkers[index].isAlive();
    }

    public void recoverShuffleWorker(int index) throws Exception {
        checkState(
                !isShuffleWorkerAlive(index),
                String.format(
                        "ShuffleWorker %s is alive now, should not be recovered.",
                        shuffleWorkers[index].getName()));
        shuffleWorkers[index] =
                new ShuffleWorkerProcess(
                        logDirName,
                        config,
                        index,
                        shuffleWorkers[index].getDataPort(),
                        shuffleWorkers[index].getDataDir());
        shuffleWorkers[index].startProcess();
    }

    public void killShuffleManager() {
        shuffleManager.destroy();
    }

    public void recoverShuffleManager() throws Exception {
        checkState(
                !shuffleManager.isAlive(),
                "ShuffleManager is still alive, should not be recovered");

        shuffleManager = new ShuffleManagerProcess(logDirName, config);
        shuffleManager.startProcess();
    }

    public Configuration getConfig() {
        return config;
    }

    public void checkResourceReleased() throws Exception {
        CommonTestUtils.delayCheck(
                () -> {
                    checkStorageResourceReleased();
                    checkNetworkReleased();
                    checkBuffersReleased();
                },
                Deadline.fromNow(Duration.ofMinutes(5)),
                "timeout");
    }

    public void checkStorageResourceReleased() {
        for (int i = 0; i < shuffleWorkers.length; ++i) {
            if (!shuffleWorkers[i].isAlive()) {
                continue;
            }
            String dataDir = getDataDirForWorker(i);
            Path dataPath = new File(dataDir).toPath();
            Path metaPath = new File(dataDir + "/_meta").toPath();
            checkUntil(
                    () -> {
                        List<Path> paths = Files.list(dataPath).collect(Collectors.toList());
                        if (!(paths.size() == 1 && paths.get(0).equals(metaPath))) {
                            StringBuilder msgBuilder = new StringBuilder("Leaking files:");
                            paths.forEach(path -> msgBuilder.append(" " + path));
                            throw new AssertionError(msgBuilder.toString());
                        }
                        if (Files.exists(metaPath)) {
                            List<Path> metas = Files.list(metaPath).collect(Collectors.toList());
                            if (!metas.isEmpty()) {
                                StringBuilder msgBuilder = new StringBuilder("Leaking metas:");
                                metas.forEach(path -> msgBuilder.append(" " + path));
                                throw new AssertionError(msgBuilder.toString());
                            }
                        }
                    });
        }
    }

    public void checkNetworkReleased() throws Exception {
        for (ShuffleWorkerProcess worker : shuffleWorkers) {
            if (!worker.isAlive()) {
                continue;
            }
            int dataPort = worker.getDataPort();
            String cmd = String.format("netstat -ant");
            Process process = Runtime.getRuntime().exec(cmd);
            StringWriter processOutput = new StringWriter();
            new CommonTestUtils.PipeForwarder(process.getInputStream(), processOutput).join();
            String[] lines = processOutput.toString().split("\n");
            for (String line : lines) {
                String pattern = String.format(".*[^0-9]%d[^0-9].*ESTABLISHED.*", dataPort);
                if (Pattern.matches(pattern, line)) {
                    throw new AssertionError("Network connections residual.");
                }
            }
        }
    }

    public void checkBuffersReleased() {
        checkUntil(
                () -> {
                    int numReadingBuffers =
                            calculateNumBuffers(MemoryOptions.MEMORY_SIZE_FOR_DATA_READING);
                    int numWritingBuffers =
                            calculateNumBuffers(MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING);

                    Collection<ShuffleWorkerMetrics> shuffleWorkerMetrics =
                            shuffleManagerClient.getShuffleWorkerMetrics().get().values();
                    for (ShuffleWorkerMetrics metric : shuffleWorkerMetrics) {
                        // check reading buffer.
                        assertThat(
                                metric.getIntegerMetric(
                                        ShuffleWorkerMetricKeys.AVAILABLE_READING_BUFFERS_KEY),
                                is(numReadingBuffers));

                        // check writing buffer.
                        assertThat(
                                metric.getIntegerMetric(
                                        ShuffleWorkerMetricKeys.AVAILABLE_READING_BUFFERS_KEY),
                                is(numWritingBuffers));
                    }
                });
    }

    private int calculateNumBuffers(ConfigOption<MemorySize> memorySizeOptions) {
        MemorySize memorySize = config.getMemorySize(memorySizeOptions);
        return CommonUtils.checkedDownCast(memorySize.getBytes() / bufferSize.getBytes());
    }

    private void checkUntil(RunnableWithException runnable) {
        Throwable lastThrowable = null;
        for (int i = 0; i < 100; i++) {
            try {
                runnable.run();
                return;
            } catch (Throwable t) {
                lastThrowable = t;
                try {
                    Thread.sleep(2000);
                } catch (Exception it) {
                }
            }
        }
        ExceptionUtils.rethrowAsRuntimeException(lastThrowable);
    }

    private void initConfig() throws Exception {
        config.addAll(ZooKeeperTestUtils.createZooKeeperHAConfig(zkConnect));
        config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
        config.setMemorySize(ManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ofMebiBytes(128));
        config.setMemorySize(ManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(32));
        config.setMemorySize(WorkerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ofMebiBytes(128));
        config.setMemorySize(WorkerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ofMebiBytes(32));
        config.setDuration(HeartbeatOptions.HEARTBEAT_JOB_INTERVAL, Duration.ofSeconds(3));
        config.setDuration(HeartbeatOptions.HEARTBEAT_WORKER_INTERVAL, Duration.ofSeconds(3));

        zkClient = ZooKeeperTestUtils.createZKClientForRemoteShuffle(config);
    }

    private void waitForShuffleClusterReady(Duration timeout) throws Exception {
        final String errorMsg =
                "LocalShuffleCluster is not ready within " + timeout.toMillis() + "ms";
        CommonTestUtils.waitUntilCondition(
                () -> shuffleManagerClient.getNumberOfRegisteredWorkers().get() >= numWorkers,
                Deadline.fromNow(timeout),
                errorMsg);
    }

    private class ShutDownFatalErrorHandler implements FatalErrorHandler {

        @Override
        public void onFatalError(Throwable exception) {
            LOG.warn(
                    "Error in LocalShuffleCluster. Shutting the LocalShuffleCluster down.",
                    exception);
            try {
                shutdown();
            } catch (Throwable throwable) {
                LOG.warn("Shutting down LocalShuffleCluster error.", throwable);
            }
        }
    }
}
