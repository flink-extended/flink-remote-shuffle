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

package com.alibaba.flink.shuffle.examples;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Batch-shuffle job demo. */
public class BatchJobDemo {
    public static void main(String[] args) throws Exception {
        int numRecords = 1024;
        int parallelism = 4;
        int recordSize = 1024;
        int numRecordsToSend = 1024;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        DataStream<byte[]> source =
                env.addSource(new ByteArraySource(numRecordsToSend, recordSize, numRecords));
        source.rebalance()
                .addSink(
                        new SinkFunction<byte[]>() {
                            @Override
                            public void invoke(byte[] value) {
                                try {
                                    System.out.println(new String(value));
                                    Thread.sleep(1000);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
        StreamGraph streamGraph = env.getStreamGraph();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        streamGraph.setJobType(JobType.BATCH);
        env.execute(streamGraph);
    }

    private static class ByteArraySource implements ParallelSourceFunction<byte[]> {
        private final int numRecordsToSend;
        private final List<byte[]> records = new ArrayList<>();
        private volatile boolean isRunning = true;

        ByteArraySource(int numRecordsToSend, int recordSize, int numRecords) {
            this.numRecordsToSend = numRecordsToSend;
            Random random = new Random();
            for (int i = 0; i < numRecords; ++i) {
                byte[] record = new byte[recordSize];
                random.nextBytes(record);
                records.add(record);
            }
        }

        @Override
        public void run(SourceContext<byte[]> sourceContext) {
            int counter = 0;
            while (isRunning && counter++ < numRecordsToSend) {
                sourceContext.collect(records.get(counter % records.size()));
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
