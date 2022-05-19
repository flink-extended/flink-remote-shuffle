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

package com.alibaba.flink.shuffle.plugin.itcase;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;

/** IT case for adaptive batch scheduler which covers cases with forward edge. */
public class AdaptiveBatchJobSchedulerWithForwardEdgeITCase extends BatchJobITCaseBase {

    private static final int DEFAULT_MAX_PARALLELISM = 100;
    private static final int SOURCE_PARALLELISM_1 = 8;
    private static final int SOURCE_PARALLELISM_2 = 12;
    private static final int NUMBERS_TO_PRODUCE = 10000;

    @Override
    void setup() {
        CountSink.countResults.clear();

        // adaptive batch job scheduler config.
        flinkConfiguration.set(
                JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
        flinkConfiguration.setInteger(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM,
                DEFAULT_MAX_PARALLELISM);
        flinkConfiguration.set(
                JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK,
                MemorySize.parse("128k"));
        numTaskManagers = 5;
        numSlotsPerTaskManager = 2;
    }

    @Test
    public void testAdaptiveBatchJobSchedulerWithForwardEdge() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(-1);
        env.setBufferTimeout(-1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        JobGraph jobGraph = createJobGraph(env);

        JobID jobID = flinkCluster.submitJob(jobGraph).get().getJobID();
        JobResult jobResult = flinkCluster.requestJobResult(jobID).get();
        if (jobResult.getSerializedThrowable().isPresent()) {
            throw new AssertionError(jobResult.getSerializedThrowable().get());
        }
        checkCountResults();
    }

    private static void checkCountResults() {
        Map<Long, Long> countResults = CountSink.countResults;

        Map<Long, Long> expectedResults =
                LongStream.range(0, NUMBERS_TO_PRODUCE)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> 2L));

        assertEquals(countResults.size(), NUMBERS_TO_PRODUCE);
        assertEquals(countResults, expectedResults);
    }

    /**
     * Create job vertices and connect them as the following JobGraph:
     *
     * <pre>
     *             hash               forward
     * 	source1 ----------> middle -----------> sink
     * 	                                        /
     * 	                    hash               /
     *  source2 ----------------------------->/
     *
     * </pre>
     *
     * <p>All edges are BLOCKING. Edge (source1 --> middle) is hash. Edge (middle --> sink) is
     * forward. Edge (source2 --> sink) is hash.
     */
    private JobGraph createJobGraph(StreamExecutionEnvironment env) throws Exception {

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

        source1.map(number -> number)
                .name("middle")
                .slotSharingGroup("group3")
                .forward()
                .union(source2)
                .addSink(new CountSink())
                .name("sink")
                .slotSharingGroup("group4");

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        streamGraph.setJobType(JobType.BATCH);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private static class CountSink implements SinkFunction<Long> {

        private static final long serialVersionUID = -4152019990787526775L;

        private static final Map<Long, Long> countResults = new ConcurrentHashMap<>();

        @Override
        public void invoke(Long value, Context context) throws Exception {
            synchronized (countResults) {
                countResults.put(value, countResults.getOrDefault(value, 0L) + 1L);
            }
        }
    }
}
