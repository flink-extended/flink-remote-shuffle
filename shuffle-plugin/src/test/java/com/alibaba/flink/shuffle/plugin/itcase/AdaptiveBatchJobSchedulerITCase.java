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

package com.alibaba.flink.shuffle.plugin.itcase;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

/** IT case for adaptive batch scheduler. */
public class AdaptiveBatchJobSchedulerITCase extends BatchJobITCaseBase {

    private static final int DEFAULT_MAX_PARALLELISM = 4;
    private static final int SOURCE_PARALLELISM_1 = 2;
    private static final int SOURCE_PARALLELISM_2 = 8;
    private static final int NUMBERS_TO_PRODUCE = 10000;

    private static ConcurrentLinkedQueue<Map<Long, Long>> numberCountResults;

    private Map<Long, Long> expectedResult;

    @Override
    public void setup() {
        expectedResult =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 2L));

        numberCountResults = new ConcurrentLinkedQueue<>();

        // adaptive batch job scheduler config.
        flinkConfiguration.set(
                JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
        flinkConfiguration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM,
                DEFAULT_MAX_PARALLELISM);
        flinkConfiguration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK,
                MemorySize.parse("256k"));
    }

    @Test
    public void testAdaptiveBatchJobScheduler() throws Exception {
        executeJob();

        Map<Long, Long> numberCountResultMap =
                numberCountResults.stream()
                        .flatMap(map -> map.entrySet().stream())
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey, Map.Entry::getValue, Long::sum));
        assertEquals(expectedResult, numberCountResultMap);
    }

    private void executeJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(-1);
        env.setBufferTimeout(-1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Long> source1 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(SOURCE_PARALLELISM_1)
                        .name("source1")
                        .slotSharingGroup("group1");

        final DataStream<Long> source2 =
                env.fromSequence(0, NUMBERS_TO_PRODUCE - 1)
                        .setParallelism(SOURCE_PARALLELISM_2)
                        .name("source2")
                        .slotSharingGroup("group2");

        source1.union(source2)
                .rescale()
                .map(new NumberCounter())
                .name("map")
                .slotSharingGroup("group3");

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        streamGraph.setJobType(JobType.BATCH);
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        JobID jobID = flinkCluster.submitJob(jobGraph).get().getJobID();
        JobResult jobResult = flinkCluster.requestJobResult(jobID).get();
        if (jobResult.getSerializedThrowable().isPresent()) {
            throw new AssertionError(jobResult.getSerializedThrowable().get());
        }
    }

    private static class NumberCounter extends RichMapFunction<Long, Long> {

        private static final long serialVersionUID = -3329103026044705105L;

        private final Map<Long, Long> numberCountResult = new HashMap<>();

        @Override
        public Long map(Long value) throws Exception {
            numberCountResult.put(value, numberCountResult.getOrDefault(value, 0L) + 1L);

            return value;
        }

        @Override
        public void close() throws Exception {
            numberCountResults.add(numberCountResult);
        }
    }
}
