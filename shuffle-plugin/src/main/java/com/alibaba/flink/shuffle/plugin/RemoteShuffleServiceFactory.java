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

package com.alibaba.flink.shuffle.plugin;

import com.alibaba.flink.shuffle.plugin.transfer.RemoteShuffleInputGateFactory;
import com.alibaba.flink.shuffle.plugin.transfer.RemoteShuffleResultPartitionFactory;
import com.alibaba.flink.shuffle.plugin.utils.ConfigurationUtils;
import com.alibaba.flink.shuffle.transfer.NettyConfig;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import java.time.Duration;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerShuffleMetrics;

/** Flink remote shuffle service implementation. */
public class RemoteShuffleServiceFactory
        implements ShuffleServiceFactory<
                RemoteShuffleDescriptor, ResultPartitionWriter, IndexedInputGate> {

    @Override
    public ShuffleMaster<RemoteShuffleDescriptor> createShuffleMaster(
            ShuffleMasterContext shuffleMasterContext) {
        return new RemoteShuffleMaster(shuffleMasterContext);
    }

    @Override
    public ShuffleEnvironment<ResultPartitionWriter, IndexedInputGate> createShuffleEnvironment(
            ShuffleEnvironmentContext context) {
        Configuration configuration = context.getConfiguration();
        int bufferSize = ConfigurationParserUtils.getPageSize(configuration);
        final int numBuffers =
                calculateNumberOfNetworkBuffers(context.getNetworkMemorySize(), bufferSize);

        ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
        MetricGroup metricGroup = context.getParentMetricGroup();

        int numPreferredClientThreads = 2 * ConfigurationParserUtils.getSlot(configuration);
        NettyConfig nettyConfig =
                new NettyConfig(
                        ConfigurationUtils.fromFlinkConfiguration(configuration),
                        numPreferredClientThreads);

        Duration requestSegmentsTimeout =
                Duration.ofMillis(
                        configuration.getLong(
                                NettyShuffleEnvironmentOptions
                                        .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS));
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(numBuffers, bufferSize, requestSegmentsTimeout);

        registerShuffleMetrics(metricGroup, networkBufferPool);

        String compressionCodec =
                configuration.getString(NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC);
        RemoteShuffleResultPartitionFactory resultPartitionFactory =
                new RemoteShuffleResultPartitionFactory(
                        ConfigurationUtils.fromFlinkConfiguration(configuration),
                        resultPartitionManager,
                        networkBufferPool,
                        bufferSize,
                        compressionCodec);

        RemoteShuffleInputGateFactory inputGateFactory =
                new RemoteShuffleInputGateFactory(
                        ConfigurationUtils.fromFlinkConfiguration(configuration),
                        networkBufferPool,
                        bufferSize,
                        compressionCodec);

        return new RemoteShuffleEnvironment(
                networkBufferPool,
                resultPartitionManager,
                resultPartitionFactory,
                inputGateFactory,
                nettyConfig);
    }

    private static int calculateNumberOfNetworkBuffers(MemorySize memorySize, int bufferSize) {
        long numBuffersLong = memorySize.getBytes() / bufferSize;
        if (numBuffersLong > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "The given number of memory bytes ("
                            + memorySize.getBytes()
                            + ") corresponds to more than MAX_INT pages.");
        }
        return (int) numBuffersLong;
    }
}
