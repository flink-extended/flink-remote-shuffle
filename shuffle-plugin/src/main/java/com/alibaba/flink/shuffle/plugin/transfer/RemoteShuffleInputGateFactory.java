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

package com.alibaba.flink.shuffle.plugin.transfer;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** Factory class to create {@link RemoteShuffleInputGate}. */
public class RemoteShuffleInputGateFactory {

    public static final int MIN_BUFFERS_PER_GATE = 16;

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleInputGateFactory.class);

    /** Number of max concurrent reading channels. */
    private final int numConcurrentReading;

    /** Codec used for compression / decompression. */
    private final String compressionCodec;

    /** Network buffer size. */
    private final int networkBufferSize;

    /**
     * Network buffer pool used for shuffle read buffers. {@link BufferPool}s will be created from
     * it and each of them will be used by a channel exclusively.
     */
    private final NetworkBufferPool networkBufferPool;

    /** Sum of buffers. */
    private final int numBuffersPerGate;

    /** Whether to shuffle input channels before reading. */
    private final boolean shuffleChannels;

    public RemoteShuffleInputGateFactory(
            Configuration configuration,
            NetworkBufferPool networkBufferPool,
            int networkBufferSize,
            String compressionCodec) {
        MemorySize configuredMemorySize =
                configuration.getMemorySize(PluginOptions.MEMORY_PER_INPUT_GATE);
        if (configuredMemorySize.getBytes() < PluginOptions.MIN_MEMORY_PER_GATE.getBytes()) {
            throw new ConfigurationException(
                    String.format(
                            "Insufficient network memory per input gate, please increase %s to at "
                                    + "least %s.",
                            PluginOptions.MEMORY_PER_INPUT_GATE.key(),
                            PluginOptions.MIN_MEMORY_PER_GATE.toHumanReadableString()));
        }

        this.numBuffersPerGate =
                CommonUtils.checkedDownCast(configuredMemorySize.getBytes() / networkBufferSize);
        if (numBuffersPerGate < MIN_BUFFERS_PER_GATE) {
            throw new ConfigurationException(
                    String.format(
                            "Insufficient network memory per input gate, please increase %s to at "
                                    + "least %d bytes.",
                            PluginOptions.MEMORY_PER_INPUT_GATE.key(),
                            networkBufferSize * MIN_BUFFERS_PER_GATE));
        }

        this.shuffleChannels = configuration.getBoolean(PluginOptions.SHUFFLE_READING_CHANNELS);
        this.compressionCodec = compressionCodec;
        this.networkBufferSize = networkBufferSize;
        this.numConcurrentReading = configuration.getInteger(PluginOptions.NUM_CONCURRENT_READINGS);
        this.networkBufferPool = networkBufferPool;
    }

    /** Create {@link RemoteShuffleInputGate} from {@link InputGateDeploymentDescriptor}. */
    public RemoteShuffleInputGate create(
            String owningTaskName,
            int gateIndex,
            InputGateDeploymentDescriptor igdd,
            ConnectionManager connectionManager) {
        LOG.info(
                "Create input gate -- number of buffers per input gate={}, "
                        + "number of concurrent readings={}.",
                numBuffersPerGate,
                numConcurrentReading);

        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(networkBufferPool, numBuffersPerGate);
        BufferDecompressor bufferDecompressor =
                new BufferDecompressor(networkBufferSize, compressionCodec);

        return createInputGate(
                owningTaskName,
                shuffleChannels,
                gateIndex,
                igdd,
                numConcurrentReading,
                connectionManager,
                bufferPoolFactory,
                bufferDecompressor);
    }

    // For testing.
    RemoteShuffleInputGate createInputGate(
            String owningTaskName,
            boolean shuffleChannels,
            int gateIndex,
            InputGateDeploymentDescriptor igdd,
            int numConcurrentReading,
            ConnectionManager connectionManager,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            BufferDecompressor bufferDecompressor) {
        return new RemoteShuffleInputGate(
                owningTaskName,
                shuffleChannels,
                gateIndex,
                networkBufferSize,
                igdd,
                numConcurrentReading,
                connectionManager,
                bufferPoolFactory,
                bufferDecompressor);
    }

    private SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            BufferPoolFactory bufferPoolFactory, int numBuffers) {
        return () -> bufferPoolFactory.createBufferPool(numBuffers, numBuffers);
    }
}
