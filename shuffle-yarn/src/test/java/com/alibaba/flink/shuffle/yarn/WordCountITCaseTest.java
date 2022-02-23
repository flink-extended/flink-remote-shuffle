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

import com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions.HA_MODE;
import static com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM;
import static com.alibaba.flink.shuffle.yarn.utils.TestTimeoutUtils.waitAllCompleted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** A simple word-count integration test. */
public class WordCountITCaseTest extends BatchJobTestBase {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountITCaseTest.class);

    private static final int NUM_WORDS = 20;

    private static final int WORD_COUNT = 2000;

    @Override
    public void setup() throws Exception {
        asyncSetupShuffleClusterOnYarn();

        configuration.setString(HA_MODE, "ZOOKEEPER");
        configuration.setString(
                HA_ZOOKEEPER_QUORUM, YarnTestBase.zookeeperTestServer.getConnectString());

        flinkConfiguration.setString(
                "shuffle-service-factory.class", RemoteShuffleServiceFactory.class.getName());
        flinkConfiguration.setString("remote-shuffle.job.memory-per-gate", "8m");

        flinkConfiguration.setString(HA_MODE.key(), "ZOOKEEPER");
        flinkConfiguration.setString(
                HA_ZOOKEEPER_QUORUM.key(), YarnTestBase.zookeeperTestServer.getConnectString());
        flinkConfiguration.setString("remote-shuffle.job.memory-per-partition", "8m");
        flinkConfiguration.setString("remote-shuffle.job.concurrent-readings-per-gate", "5");
    }

    private static void asyncSetupShuffleClusterOnYarn() throws Exception {
        long start = System.currentTimeMillis();
        CompletableFuture<Boolean> setupFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                RemoteShuffleOnYarnTestCluster.setup();
                                return true;
                            } catch (Exception e) {
                                e.printStackTrace();
                                LOG.error("Failed to setup shuffle cluster on Yarn, ", e);
                                return false;
                            }
                        });
        List<Boolean> results =
                waitAllCompleted(Collections.singletonList(setupFuture), 600, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - start;
        LOG.info("The process of setting up shuffle cluster on Yarn took " + duration + " ms");
        assertEquals(1, results.size());
        assertTrue(results.get(0));
    }

    @Override
    void shutdown() {
        RemoteShuffleOnYarnTestCluster.shutdown();
    }

    @Test(timeout = 600000L)
    public void testWordCount() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(flinkConfiguration);

        int parallelism = numTaskManagers * numSlotsPerTaskManager;
        env.getConfig().setExecutionMode(ExecutionMode.BATCH);
        env.getConfig().setParallelism(parallelism);
        env.getConfig().setDefaultInputDependencyConstraint(InputDependencyConstraint.ALL);
        env.disableOperatorChaining();

        DataStream<Tuple2<String, Long>> words =
                env.fromSequence(0, NUM_WORDS)
                        .broadcast()
                        .map(new WordsMapper())
                        .flatMap(new WordsFlatMapper(WORD_COUNT));
        words.keyBy(value -> value.f0)
                .sum(1)
                .map((MapFunction<Tuple2<String, Long>, Long>) wordCount -> wordCount.f1)
                .addSink(new VerifySink(parallelism * WORD_COUNT));

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

    private static class WordsMapper implements MapFunction<Long, String> {

        private static final long serialVersionUID = 5666190363617738047L;

        private static final String WORD_SUFFIX_1K = getWordSuffix1k();

        private static String getWordSuffix1k() {
            StringBuilder builder = new StringBuilder();
            builder.append("-");
            for (int i = 0; i < 1024; ++i) {
                builder.append("0");
            }
            return builder.toString();
        }

        @Override
        public String map(Long value) {
            return "WORD-" + value + WORD_SUFFIX_1K;
        }
    }

    private static class WordsFlatMapper implements FlatMapFunction<String, Tuple2<String, Long>> {

        private static final long serialVersionUID = -1503963599349890992L;

        private final int wordsCount;

        public WordsFlatMapper(int wordsCount) {
            checkArgument(wordsCount > 0, "Must be positive.");
            this.wordsCount = wordsCount;
        }

        @Override
        public void flatMap(String word, Collector<Tuple2<String, Long>> collector) {
            for (int i = 0; i < wordsCount; ++i) {
                collector.collect(new Tuple2<>(word, 1L));
            }
        }
    }

    private static class VerifySink implements SinkFunction<Long> {

        private static final long serialVersionUID = -1504978632259778200L;

        private final Long wordCount;

        public VerifySink(long wordCount) {
            this.wordCount = wordCount;
        }

        @Override
        public void invoke(Long value, SinkFunction.Context context) {
            assertEquals(wordCount, value);
        }
    }
}
