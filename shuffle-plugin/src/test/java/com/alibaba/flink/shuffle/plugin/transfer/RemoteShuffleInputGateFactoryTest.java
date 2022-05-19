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

package com.alibaba.flink.shuffle.plugin.transfer;

import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.config.PluginOptions;
import com.alibaba.flink.shuffle.plugin.utils.ConfigurationUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ShuffleReadClient;
import com.alibaba.flink.shuffle.transfer.TransferBufferPool;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.function.Consumer;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Test for {@link RemoteShuffleInputGateFactory}. */
public class RemoteShuffleInputGateFactoryTest {

    @Test
    public void testBasicRoutine() throws Exception {
        testCreateInputGateAndBufferPool(16, "16m");
    }

    @Test
    public void testLessThanMinMemoryPerGate() {
        Exception e =
                assertThrows(
                        ConfigurationException.class,
                        () -> testCreateInputGateAndBufferPool(16, "2m"));
        String msg = "Insufficient network memory per input gate, please increase";
        assertTrue(e.getMessage().contains(msg));
    }

    @Test
    public void testLessThanMinBuffersPerGate() {
        Exception e =
                assertThrows(
                        ConfigurationException.class,
                        () -> testCreateInputGateAndBufferPool(8, "8m"));
        String msg = "Insufficient network memory per input gate, please increase";
        assertTrue(e.getMessage().contains(msg));
    }

    @Test
    public void testNetworkBufferShortage() {
        IOException e =
                assertThrows(IOException.class, () -> testCreateInputGateAndBufferPool(8, "16m"));
        assertTrue(e.getMessage().contains("Insufficient number of network buffers"));
    }

    private void testCreateInputGateAndBufferPool(int totalNumNetworkBuffers, String inputMemory)
            throws Exception {
        int bufferSize = 1024 * 1024;
        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(totalNumNetworkBuffers, bufferSize);
        Configuration conf = new Configuration();
        conf.setString(PluginOptions.MEMORY_PER_INPUT_GATE.key(), inputMemory);
        RemoteShuffleInputGateFactory factory =
                new TestingRemoteShuffleInputGateFactory(
                        ConfigurationUtils.fromFlinkConfiguration(conf),
                        networkBufferPool,
                        bufferSize);
        TestingRemoteShuffleInputGate gate =
                (TestingRemoteShuffleInputGate)
                        factory.create("", 0, createGateDescriptor(100), null);
        BufferPool bp = gate.bufferPoolFactory.get();
        bp.lazyDestroy();
        networkBufferPool.destroy();
    }

    private InputGateDeploymentDescriptor createGateDescriptor(int numShuffleDescs)
            throws Exception {
        int subIdx = 99;
        return new InputGateDeploymentDescriptor(
                new IntermediateDataSetID(),
                ResultPartitionType.BLOCKING,
                subIdx,
                createShuffleDescriptors(numShuffleDescs));
    }

    private ShuffleDescriptor[] createShuffleDescriptors(int num) throws Exception {
        JobID jID = new JobID(CommonUtils.randomBytes(8));
        RemoteShuffleDescriptor[] ret = new RemoteShuffleDescriptor[num];
        for (int i = 0; i < num; i++) {
            ResultPartitionID rID = new ResultPartitionID();
            ShuffleResource resource =
                    new DefaultShuffleResource(
                            new ShuffleWorkerDescriptor[] {
                                new ShuffleWorkerDescriptor(
                                        null, InetAddress.getLocalHost().getHostAddress(), 0)
                            },
                            DataPartition.DataPartitionType.MAP_PARTITION);
            ret[i] = new RemoteShuffleDescriptor(rID, jID, resource);
        }
        return ret;
    }

    private static class TestingRemoteShuffleInputGateFactory
            extends RemoteShuffleInputGateFactory {

        private final int bufferSize;

        TestingRemoteShuffleInputGateFactory(
                com.alibaba.flink.shuffle.common.config.Configuration configuration,
                NetworkBufferPool networkBufferPool,
                int bufferSize) {
            super(configuration, networkBufferPool, bufferSize, "LZ4");
            this.bufferSize = bufferSize;
        }

        @Override
        RemoteShuffleInputGate createInputGate(
                String owningTaskName,
                boolean shuffleChannels,
                int gateIndex,
                InputGateDeploymentDescriptor igdd,
                int numConcurrentReading,
                ConnectionManager connectionManager,
                SupplierWithException<BufferPool, IOException> bufferPoolFactory,
                BufferDecompressor bufferDecompressor) {
            return new TestingRemoteShuffleInputGate(
                    owningTaskName,
                    shuffleChannels,
                    gateIndex,
                    bufferSize,
                    igdd,
                    numConcurrentReading,
                    connectionManager,
                    bufferPoolFactory,
                    bufferDecompressor);
        }
    }

    private static class TestingRemoteShuffleInputGate extends RemoteShuffleInputGate {

        private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

        TestingRemoteShuffleInputGate(
                String taskName,
                boolean shuffleChannels,
                int gateIndex,
                int networkBufferSize,
                InputGateDeploymentDescriptor gateDescriptor,
                int numConcurrentReading,
                ConnectionManager connectionManager,
                SupplierWithException<BufferPool, IOException> bufferPoolFactory,
                BufferDecompressor bufferDecompressor) {
            super(
                    taskName,
                    shuffleChannels,
                    gateIndex,
                    networkBufferSize,
                    gateDescriptor,
                    numConcurrentReading,
                    connectionManager,
                    bufferPoolFactory,
                    bufferDecompressor);
            this.bufferPoolFactory = bufferPoolFactory;
        }

        @Override
        ShuffleReadClient createShuffleReadClient(
                ConnectionManager connectionManager,
                InetSocketAddress address,
                DataSetID dataSetID,
                MapPartitionID mapID,
                int startSubIdx,
                int endSubIdx,
                int bufferSize,
                TransferBufferPool bufferPool,
                Consumer<ByteBuf> dataListener,
                Consumer<Throwable> failureListener) {
            return null;
        }
    }
}
