/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.e2e;

import com.alibaba.flink.shuffle.common.functions.ConsumerWithException;
import com.alibaba.flink.shuffle.e2e.flinkcluster.FlinkLocalCluster;
import com.alibaba.flink.shuffle.e2e.zookeeper.ZooKeeperTestUtils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.GlobalDataExchangeMode;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.alibaba.flink.shuffle.common.utils.ProcessUtils.getProcessID;
import static org.apache.flink.streaming.api.environment.StreamExecutionEnvironment.createRemoteEnvironment;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/** Flink job for shuffle testing. */
public class JobForShuffleTesting {

    private static final Logger LOG = LoggerFactory.getLogger(JobForShuffleTesting.class);

    private static final int LARGE_SCALE_DATA_RECORDS = 30000000;

    private static final int NORMAL_SCALE_DATA_RECORDS = 3000000;

    private final int numRecords;

    private static final int EXPECT_CHECKSUM_WHEN_LARGE = 55032;

    private static final int EXPECT_CHECKSUM_WHEN_NORMAL = 50418;

    static final int PARALLELISM = 2;

    static final String STAGE0_NAME = "shuffleWrite";

    static final String STAGE1_NAME = "shuffleRead&Write";

    static final String STAGE2_NAME = "shuffleRead";

    private final FlinkLocalCluster cluster;

    private final int parallelism;

    private final Configuration config;

    private final String outputPath;

    private final String zkConnect;

    private final String zkPath;

    private Consumer<ExecutionConfig> executionConfigModifier;

    public JobForShuffleTesting(FlinkLocalCluster cluster) throws Exception {
        this(cluster, PARALLELISM, DataScale.LARGE);
    }

    public JobForShuffleTesting(FlinkLocalCluster cluster, DataScale scale) throws Exception {
        this(cluster, PARALLELISM, scale);
    }

    public JobForShuffleTesting(FlinkLocalCluster cluster, int parallelism, DataScale scale)
            throws Exception {
        this.cluster = cluster;
        this.parallelism = parallelism;
        this.config = cluster.getConfig();
        this.outputPath = cluster.temporaryFolder.newFolder().getPath();
        this.zkConnect = cluster.getZKConnect();
        this.zkPath = cluster.getZKPath();
        switch (scale) {
            case LARGE:
                numRecords = LARGE_SCALE_DATA_RECORDS;
                break;
            case NORMAL:
            default:
                numRecords = NORMAL_SCALE_DATA_RECORDS;
        }
    }

    static class NoWatermark implements WatermarkStrategy<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public WatermarkGenerator<Long> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new NoWatermarksGenerator<>();
        }
    }

    public void run() throws Exception {
        LOG.info("Starting job to process {} records and write to {}.", numRecords, outputPath);

        StreamExecutionEnvironment env = createRemoteEnvironment("localhost", 1337, config);
        env.setParallelism(parallelism);
        env.setBufferTimeout(-1L);
        ExecutionConfig executionConfig = env.getConfig();
        executionConfig.setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
        executionConfig.setExecutionMode(ExecutionMode.BATCH);

        TupleTypeInfo<Tuple2<Long, Long>> typeInfo =
                new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

        env.fromSource(
                        new NumberSequenceSource(1, numRecords),
                        new NoWatermark(),
                        "Sequence Source")
                .map(
                        new MapFunction<Long, Tuple2<Long, Long>>() {
                            @Override
                            public Tuple2<Long, Long> map(Long aLong) throws Exception {
                                return Tuple2.of(aLong, aLong + 1);
                            }
                        })
                .transform(
                        STAGE0_NAME,
                        typeInfo,
                        new MapOpWithStub(zkConnect, zkPath, STAGE0_NAME, parallelism, numRecords))
                .name(STAGE0_NAME)
                .keyBy(0)
                .transform(
                        STAGE1_NAME,
                        typeInfo,
                        new MapOpWithStub(zkConnect, zkPath, STAGE1_NAME, parallelism, numRecords))
                .name(STAGE1_NAME)
                .keyBy(1)
                .transform(
                        STAGE2_NAME,
                        typeInfo,
                        new MapOpWithStub(zkConnect, zkPath, STAGE2_NAME, parallelism, numRecords))
                .name(STAGE2_NAME)
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        if (executionConfigModifier != null) {
            executionConfigModifier.accept(executionConfig);
        }

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalDataExchangeMode(GlobalDataExchangeMode.ALL_EDGES_BLOCKING);
        streamGraph.setJobName("Streaming WordCount");
        env.execute(streamGraph);

        verifyResult();
    }

    public void setExecutionConfigModifier(Consumer<ExecutionConfig> executionConfigModifier) {
        this.executionConfigModifier = executionConfigModifier;
    }

    public void planFailingARunningTask(String stageName, int taskID, int attemptID)
            throws Exception {
        String path = pathOfTaskCmd(stageName, taskID, attemptID, TaskStat.RUNNING);
        CuratorFramework zk = cluster.getZKClient();
        zk.create().forPath(path);
        zk.setData().forPath(path, new byte[] {(byte) TaskCmd.SELF_DESTROY.ordinal()});
    }

    public void planOperation(
            String stageName,
            int taskID,
            int attemptID,
            TaskStat taskStat,
            ConsumerWithException<ProcessIDAndTaskStat, Exception> listener)
            throws Exception {
        planAsyncOperation(
                stageName,
                taskID,
                attemptID,
                taskStat,
                (processIdAndTaskStat, nodeCache) -> {
                    if (processIdAndTaskStat.getStat() == taskStat) {
                        listener.accept(processIdAndTaskStat);
                    }
                    return CompletableFuture.completedFuture(null);
                });
    }

    public void planAsyncOperation(
            String stageName,
            int taskID,
            int attemptID,
            TaskStat taskStat,
            AsyncPlanListener listener)
            throws Exception {
        String pathCmd = pathOfTaskCmd(stageName, taskID, attemptID, taskStat);
        CuratorFramework zk = cluster.getZKClient();
        zk.create().forPath(pathCmd);
        zk.setData().forPath(pathCmd, new byte[] {(byte) TaskCmd.WAIT_KEEP_GOING.ordinal()});

        String pathInfo = pathOfTaskInfo(stageName, taskID, attemptID);
        NodeCache nodeCache = new NodeCache(zk, pathInfo, false);
        NodeCacheListener l =
                () -> {
                    ChildData childData = nodeCache.getCurrentData();
                    if (nodeCache.getCurrentData() == null || childData.getData() == null) {
                        return;
                    }
                    ProcessIDAndTaskStat processIDAndTaskStat =
                            ProcessIDAndTaskStat.fromBytes(childData.getData());

                    try {
                        if (processIDAndTaskStat.getStat() == taskStat) {
                            CompletableFuture<Void> resultFuture =
                                    listener.onPlanedTimeReached(processIDAndTaskStat, nodeCache);
                            resultFuture.whenComplete(
                                    (ret, throwable) -> {
                                        if (throwable != null) {
                                            cancelJob(throwable);
                                        } else {
                                            try {
                                                zk.setData()
                                                        .forPath(
                                                                pathCmd,
                                                                new byte[] {
                                                                    (byte)
                                                                            TaskCmd.KEEP_GOING
                                                                                    .ordinal()
                                                                });
                                            } catch (Exception e) {
                                                cancelJob(e);
                                            }
                                        }
                                    });
                        }
                    } catch (Throwable throwable) {
                        // Exception caught, let's fail the job
                        LOG.error("The check failed and would stop the flink cluster", throwable);
                        cancelJob(throwable);
                    }
                };

        nodeCache.start();
        nodeCache.getListenable().addListener(l);
    }

    private void cancelJob(Throwable throwable) {
        try {
            // If check failed, we would then cancel all the jobs to make the test
            // fail...
            RestClusterClient<StandaloneClusterId> restClusterClient =
                    new RestClusterClient<>(config, StandaloneClusterId.getInstance());
            Collection<JobStatusMessage> jobStatusMessages = restClusterClient.listJobs().get();
            for (JobStatusMessage jobStatusMessage : jobStatusMessages) {
                LOG.error("Canceling " + jobStatusMessage.getJobId() + " due to ", throwable);
                restClusterClient.cancel(jobStatusMessage.getJobId()).get();
            }

            restClusterClient.close();
        } catch (Exception e) {
            LOG.info("Failed to cancel job.", e);
        }
    }

    public void checkNoResult() throws Exception {
        if (Files.list(new File(outputPath).toPath()).findAny().isPresent()) {
            throw new AssertionError("Result exists.");
        }
    }

    private static final class MapOpWithStub extends AbstractStreamOperator<Tuple2<Long, Long>>
            implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private static final long serialVersionUID = -4276646161067243517L;

        private final String zkConnect;

        private final String zkPath;

        private final String name;

        private final int parallelism;

        private Environment env;

        private int numRecordsProcessed;

        private CuratorFramework zkClient;

        private int taskIdx;

        private int attempt;

        private final int numRecords;

        public MapOpWithStub(
                String zkConnect, String zkPath, String name, int parallelism, int numRecords) {
            this.zkConnect = zkConnect;
            this.zkPath = zkPath;
            this.name = name;
            this.parallelism = parallelism;
            this.numRecords = numRecords;
            this.setChainingStrategy(ChainingStrategy.ALWAYS);
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Tuple2<Long, Long>>> output) {
            super.setup(containingTask, config, output);

            this.env = containingTask.getEnvironment();
        }

        @Override
        public void open() throws Exception {
            super.open();
            Configuration conf =
                    ZooKeeperTestUtils.createZooKeeperHAConfigForFlink(zkConnect, zkPath);
            zkClient = ZooKeeperTestUtils.createZKClientForFlink(conf);
            taskIdx = getRuntimeContext().getIndexOfThisSubtask();
            attempt = getRuntimeContext().getAttemptNumber();
            final ResultPartitionID resultPartitionID;
            if (env.getAllWriters().length > 0) {
                resultPartitionID = env.getWriter(0).getPartitionId();
            } else {
                resultPartitionID = null;
            }
            ProcessIDAndTaskStat data =
                    new ProcessIDAndTaskStat(resultPartitionID, getProcessID(), TaskStat.OPENED);
            zkClient.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(pathOfTaskInfo(name, taskIdx, attempt), data.toBytes());
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Long, Long>> streamRecord) {
            if (numRecordsProcessed == numRecords / parallelism * 0.5) {
                try {
                    final ResultPartitionID resultPartitionID;
                    if (env.getAllWriters().length > 0) {
                        resultPartitionID = env.getWriter(0).getPartitionId();
                    } else {
                        resultPartitionID = null;
                    }
                    ProcessIDAndTaskStat data =
                            new ProcessIDAndTaskStat(
                                    resultPartitionID, getProcessID(), TaskStat.RUNNING);
                    zkClient.setData()
                            .forPath(pathOfTaskInfo(name, taskIdx, attempt), data.toBytes());
                    TaskCmd cmd = readCmd(TaskStat.RUNNING);
                    if (cmd != null) {
                        execCmd(cmd, TaskStat.RUNNING);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            numRecordsProcessed++;

            int mod = 99999;
            Tuple2<Long, Long> longs = streamRecord.getValue();
            output.collect(streamRecord.replace(Tuple2.of(longs.f0 * 2 % mod, longs.f1 * 3 % mod)));
        }

        @Override
        public void close() throws Exception {
            super.close();

            final ResultPartitionID resultPartitionID;
            if (env.getAllWriters().length > 0) {
                resultPartitionID = env.getWriter(0).getPartitionId();
            } else {
                resultPartitionID = null;
            }
            ProcessIDAndTaskStat data =
                    new ProcessIDAndTaskStat(resultPartitionID, getProcessID(), TaskStat.CLOSED);
            zkClient.setData().forPath(pathOfTaskInfo(name, taskIdx, attempt), data.toBytes());
            zkClient.close();
        }

        private TaskCmd readCmd(TaskStat taskStat) throws Exception {
            TaskCmd cmd = null;
            String cmdPath = pathOfTaskCmd(name, taskIdx, attempt, taskStat);
            if (zkClient.checkExists().forPath(cmdPath) != null) {
                byte b = zkClient.getData().forPath(cmdPath)[0];
                cmd = TaskCmd.values()[b];
            }
            return cmd;
        }

        private void execCmd(TaskCmd cmd, TaskStat taskStat) throws Exception {
            switch (cmd) {
                case KEEP_GOING:
                    break;
                case WAIT_KEEP_GOING:
                    while (readCmd(taskStat) == TaskCmd.WAIT_KEEP_GOING) {
                        Thread.sleep(1000);
                    }
                    break;
                case SELF_DESTROY:
                    try {
                        zkClient.delete().forPath(pathOfTaskCmd(name, taskIdx, attempt, taskStat));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    throw new RuntimeException("Task self destroy.");
            }
        }
    }

    static String pathOfTaskInfo(String name, int taskIdx, int attempt) {
        return "/" + name + "-task" + taskIdx + "-attempt" + attempt + ".info";
    }

    private static String pathOfTaskCmd(String name, int taskIdx, int attempt, TaskStat taskStat) {
        return "/" + name + "-task" + taskIdx + "-attempt" + attempt + "-" + taskStat + ".cmd";
    }

    private enum TaskCmd {
        KEEP_GOING,
        WAIT_KEEP_GOING,
        SELF_DESTROY;
    }

    /** Task status when running. */
    public static class ProcessIDAndTaskStat {

        /** Only the single result partition id is recorded. */
        ResultPartitionID resultPartitionID;

        int processID;

        TaskStat stat;

        public ProcessIDAndTaskStat(
                ResultPartitionID resultPartitionID, int processID, TaskStat stat) {
            this.resultPartitionID = resultPartitionID;
            this.processID = processID;
            this.stat = stat;
        }

        public int getProcessID() {
            return processID;
        }

        public TaskStat getStat() {
            return stat;
        }

        static ProcessIDAndTaskStat fromBytes(byte[] bytes) {

            ByteBuffer wrapped = ByteBuffer.wrap(bytes);
            int resultPartitionIDBytesSize = wrapped.getInt();
            final ResultPartitionID resultPartitionID;
            if (resultPartitionIDBytesSize > 0) {
                byte[] resultPartitionIDBytes = new byte[resultPartitionIDBytesSize];
                wrapped.get(resultPartitionIDBytes);

                ByteBuf resultPartitionIDByteBuf = Unpooled.wrappedBuffer(resultPartitionIDBytes);
                IntermediateResultPartitionID intermediateResultPartitionID =
                        IntermediateResultPartitionID.fromByteBuf(resultPartitionIDByteBuf);
                ExecutionAttemptID executionAttemptID =
                        ExecutionAttemptID.fromByteBuf(resultPartitionIDByteBuf);
                resultPartitionID =
                        new ResultPartitionID(intermediateResultPartitionID, executionAttemptID);
            } else {
                resultPartitionID = null;
            }
            int processID = wrapped.getInt();
            TaskStat stat = TaskStat.values()[wrapped.get()];
            return new ProcessIDAndTaskStat(resultPartitionID, processID, stat);
        }

        byte[] toBytes() {
            ByteBuf resultPartitionIDsByteBuf = Unpooled.buffer();
            if (resultPartitionID != null) {
                resultPartitionID.getPartitionId().writeTo(resultPartitionIDsByteBuf);
                resultPartitionID.getProducerId().writeTo(resultPartitionIDsByteBuf);
            }

            byte[] resultPartitionIDsBytes = new byte[resultPartitionIDsByteBuf.readableBytes()];
            resultPartitionIDsByteBuf.readBytes(resultPartitionIDsBytes);

            ByteBuffer buf = ByteBuffer.allocate(4 + resultPartitionIDsBytes.length + 5);
            buf.putInt(resultPartitionIDsBytes.length);
            buf.put(resultPartitionIDsBytes);
            buf.putInt(processID);
            buf.put((byte) stat.ordinal());
            return buf.array();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ProcessIDAndTaskStat that = (ProcessIDAndTaskStat) o;
            return processID == that.processID
                    && Objects.equals(resultPartitionID, that.resultPartitionID)
                    && stat == that.stat;
        }

        @Override
        public int hashCode() {
            return Objects.hash(resultPartitionID, processID, stat);
        }
    }

    /** Task state on ZK. */
    public enum TaskStat {
        OPENED,
        RUNNING,
        CLOSED
    }

    public int getParallelism() {
        return parallelism;
    }

    public void checkAttemptsNum(String stageName, int taskID, int expectNum) throws Exception {
        for (int i = 0; i < expectNum; i++) {
            String taskInfoPath = pathOfTaskInfo(stageName, taskID, i);
            assertNotNull(cluster.getZKClient().checkExists().forPath(taskInfoPath));
        }
        String taskInfoPath = pathOfTaskInfo(stageName, taskID, expectNum);
        assertNull(cluster.getZKClient().checkExists().forPath(taskInfoPath));
    }

    private void verifyResult() throws Exception {
        int checksum = 0;
        int numRecords = 0;
        for (int i = 1; i <= parallelism; i++) {
            try (BufferedReader br = new BufferedReader(new FileReader(outputPath + "/" + i))) {
                String line = null;
                while ((line = br.readLine()) != null) {
                    String strippedBrackets = line.substring(1, line.length() - 1);
                    String[] numbers = strippedBrackets.split(",");
                    checksum += Integer.parseInt(numbers[0]);
                    checksum %= 77777;
                    checksum += Integer.parseInt(numbers[1]);
                    checksum %= 77777;
                    numRecords += 1;
                }
            }
        }
        if (this.numRecords == NORMAL_SCALE_DATA_RECORDS
                && checksum != EXPECT_CHECKSUM_WHEN_NORMAL) {
            throw new IllegalStateException(
                    String.format(
                            "Expect checksum %d, but found %d.",
                            EXPECT_CHECKSUM_WHEN_NORMAL, checksum));
        }

        if (this.numRecords == LARGE_SCALE_DATA_RECORDS && checksum != EXPECT_CHECKSUM_WHEN_LARGE) {
            throw new IllegalStateException(
                    String.format(
                            "Expect checksum %d, but found %d.",
                            EXPECT_CHECKSUM_WHEN_LARGE, checksum));
        }

        if (numRecords != this.numRecords) {
            throw new IllegalStateException(
                    String.format(
                            "Expect numRecords %d, but found %d.", this.numRecords, numRecords));
        }
    }

    /** Asynchronous Listener of the planned time has reached. */
    public interface AsyncPlanListener {

        CompletableFuture<Void> onPlanedTimeReached(
                ProcessIDAndTaskStat taskStat, NodeCache nodeCache) throws Exception;
    }

    enum DataScale {
        NORMAL,
        LARGE
    }
}
