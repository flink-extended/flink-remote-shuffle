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
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Factory class to create {@link RemoteShuffleResultPartition}. */
public class RemoteShuffleResultPartitionFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoteShuffleResultPartitionFactory.class);

    public static final int MIN_BUFFERS_PER_PARTITION = 16;

    /** Not used and just for compatibility with Flink pluggable shuffle service. */
    private final ResultPartitionManager partitionManager;

    /** Network buffer pool used for shuffle write buffers. */
    private final BufferPoolFactory bufferPoolFactory;

    /** Network buffer size. */
    private final int networkBufferSize;

    /** Remote data partition type. */
    private final String dataPartitionFactoryName;

    /** Whether compression enabled. */
    private final boolean compressionEnabled;

    /** Codec used for compression / decompression. */
    private final String compressionCodec;

    /**
     * Configured number of buffers for shuffle write, it contains two parts: sorting buffers and
     * transportation buffers.
     */
    private final int numBuffersPerPartition;

    public RemoteShuffleResultPartitionFactory(
            Configuration configuration,
            ResultPartitionManager partitionManager,
            BufferPoolFactory bufferPoolFactory,
            int networkBufferSize,
            String compressionCodec) {
        MemorySize configuredMemorySize =
                configuration.getMemorySize(PluginOptions.MEMORY_PER_RESULT_PARTITION);
        if (configuredMemorySize.getBytes() < PluginOptions.MIN_MEMORY_PER_PARTITION.getBytes()) {
            throw new ConfigurationException(
                    String.format(
                            "Insufficient network memory per result partition, please increase %s "
                                    + "to at least %s.",
                            PluginOptions.MEMORY_PER_RESULT_PARTITION.key(),
                            PluginOptions.MIN_MEMORY_PER_PARTITION.toHumanReadableString()));
        }

        this.numBuffersPerPartition =
                CommonUtils.checkedDownCast(configuredMemorySize.getBytes() / networkBufferSize);
        if (numBuffersPerPartition < MIN_BUFFERS_PER_PARTITION) {
            throw new ConfigurationException(
                    String.format(
                            "Insufficient network memory per partition, please increase %s to at "
                                    + "least %d bytes.",
                            PluginOptions.MEMORY_PER_RESULT_PARTITION.key(),
                            networkBufferSize * MIN_BUFFERS_PER_PARTITION));
        }

        this.compressionEnabled = configuration.getBoolean(PluginOptions.ENABLE_DATA_COMPRESSION);
        this.dataPartitionFactoryName =
                configuration.getString(PluginOptions.DATA_PARTITION_FACTORY_NAME);

        this.partitionManager = partitionManager;
        this.bufferPoolFactory = bufferPoolFactory;
        this.networkBufferSize = networkBufferSize;
        this.compressionCodec = compressionCodec;
    }

    public ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionDeploymentDescriptor desc,
            ConnectionManager connectionManager) {
        LOG.info(
                "Create result partition -- number of buffers per result partition={}, "
                        + "number of subpartitions={}.",
                numBuffersPerPartition,
                desc.getNumberOfSubpartitions());

        return create(
                taskNameWithSubtaskAndId,
                partitionIndex,
                desc.getShuffleDescriptor().getResultPartitionID(),
                desc.getPartitionType(),
                desc.getNumberOfSubpartitions(),
                desc.getMaxParallelism(),
                createBufferPoolFactory(),
                desc.getShuffleDescriptor(),
                connectionManager);
    }

    private ResultPartition create(
            String taskNameWithSubtaskAndId,
            int partitionIndex,
            ResultPartitionID id,
            ResultPartitionType type,
            int numSubpartitions,
            int maxParallelism,
            List<SupplierWithException<BufferPool, IOException>> bufferPoolFactories,
            ShuffleDescriptor shuffleDescriptor,
            ConnectionManager connectionManager) {

        final BufferCompressor bufferCompressor;
        if (compressionEnabled) {
            bufferCompressor = new BufferCompressor(networkBufferSize, compressionCodec);
        } else {
            bufferCompressor = null;
        }
        RemoteShuffleDescriptor rsd = (RemoteShuffleDescriptor) shuffleDescriptor;
        ResultPartition partition =
                new RemoteShuffleResultPartition(
                        taskNameWithSubtaskAndId,
                        partitionIndex,
                        id,
                        type,
                        numSubpartitions,
                        maxParallelism,
                        networkBufferSize,
                        partitionManager,
                        bufferCompressor,
                        bufferPoolFactories.get(0),
                        new RemoteShuffleOutputGate(
                                rsd,
                                numSubpartitions,
                                networkBufferSize,
                                dataPartitionFactoryName,
                                bufferPoolFactories.get(1),
                                connectionManager));
        LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);
        return partition;
    }

    /**
     * Used to create 2 buffer pools -- sorting buffer pool (7/8), transportation buffer pool (1/8).
     */
    private List<SupplierWithException<BufferPool, IOException>> createBufferPoolFactory() {
        int numForResultPartition = numBuffersPerPartition * 7 / 8;
        int numForOutputGate = numBuffersPerPartition - numForResultPartition;

        List<SupplierWithException<BufferPool, IOException>> factories = new ArrayList<>();
        factories.add(
                () ->
                        bufferPoolFactory.createBufferPool(
                                numForResultPartition, numForResultPartition));
        factories.add(() -> bufferPoolFactory.createBufferPool(numForOutputGate, numForOutputGate));
        return factories;
    }
}
