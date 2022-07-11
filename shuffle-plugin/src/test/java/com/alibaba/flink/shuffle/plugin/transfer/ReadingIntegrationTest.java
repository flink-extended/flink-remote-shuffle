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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.DiskType;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.transfer.AbstractNettyTest;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.FakedDataPartitionReadingView;
import com.alibaba.flink.shuffle.transfer.NettyConfig;
import com.alibaba.flink.shuffle.transfer.NettyServer;
import com.alibaba.flink.shuffle.transfer.TestTransferBufferPool;
import com.alibaba.flink.shuffle.transfer.utils.NoOpPartitionedDataStore;

import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Integration test for reading process. */
@RunWith(Parameterized.class)
public class ReadingIntegrationTest {

    private String localhost;

    private Random random;

    private List<FakedPartitionDataStore> dataStores;

    private List<NettyServer> nettyServers;

    private List<ShuffleWorkerDescriptor> workerDescs;

    private NetworkBufferPool networkBufferPool;

    private final int upstreamParallelism;

    private final int numConcurrentReading;

    // Num of input gates share a ConnectionManager.
    private final int groupSize;

    private final int downStreamParallelism;

    private final int numShuffleWorkers;

    // Num of buffers per reading channel.
    private final int dataScalePerReadingView;

    private final int buffersPerClientChannelBufferPool;

    public ReadingIntegrationTest(
            int upstreamParallelism,
            int numConcurrentReading,
            int groupSize,
            int downStreamParallelism,
            int numShuffleWorkers,
            int dataScalePerReadingView,
            int buffersPerClientChannelBufferPool) {
        this.upstreamParallelism = upstreamParallelism;
        this.numConcurrentReading = numConcurrentReading;
        this.groupSize = groupSize;
        this.downStreamParallelism = downStreamParallelism;
        this.numShuffleWorkers = numShuffleWorkers;
        this.dataScalePerReadingView = dataScalePerReadingView;
        this.buffersPerClientChannelBufferPool = buffersPerClientChannelBufferPool;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {10, 10, 1, 8, 3, 100, 10},
                    {10, 10, 1, 8, 3, 0, 10},
                    {10, 10, 2, 8, 3, 100, 10},
                    {10, 10, 8, 8, 3, 100, 10},
                    {1200, 10, 4, 8, 3, 100, 10},
                    {1200, 10, 4, 8, 3, 100, 50},
                    {1200, 10, 1, 8, 3, 100, 100},
                    {1200, 10, 4, 8, 3, 100, 100}
                });
    }

    @Before
    public void setup() throws Exception {
        random = new Random();
        networkBufferPool = new NetworkBufferPool(20000, 32);
        if (localhost == null) {
            localhost = InetAddress.getLocalHost().getHostAddress();
        }
    }

    @After
    public void tearDown() {
        assertEquals(20000, networkBufferPool.getNumberOfAvailableMemorySegments());
        networkBufferPool.destroy();
        nettyServers.forEach(NettyServer::shutdown);
    }

    @Test(timeout = 600_000)
    public void test() throws Exception {
        startServers(numShuffleWorkers, dataScalePerReadingView);

        List<ConnectionManager> connMgrs = new ArrayList<>();
        List<Thread> readingThreads = new ArrayList<>();
        AtomicInteger buffersCounter = new AtomicInteger(0);
        AtomicReference<Throwable> cause = new AtomicReference<>(null);
        for (int i = 0; i < downStreamParallelism; i += groupSize) {
            List<BufferPool> bufferPools = new ArrayList<>(groupSize);
            for (int j = 0; j < groupSize; j++) {
                int numBuffers = buffersPerClientChannelBufferPool * numConcurrentReading;
                bufferPools.add(networkBufferPool.createBufferPool(numBuffers, numBuffers));
            }
            Pair<ConnectionManager, List<RemoteShuffleInputGate>> pair =
                    createInputGateGroup(
                            bufferPools, numConcurrentReading, upstreamParallelism, workerDescs);
            connMgrs.add(pair.getLeft());
            for (RemoteShuffleInputGate gate : pair.getRight()) {
                readingThreads.add(new ReadingThread(gate, buffersCounter, cause));
            }
        }
        readingThreads.forEach(Thread::start);
        for (Thread t : readingThreads) {
            t.join();
        }
        for (ConnectionManager connMgr : connMgrs) {
            assertEquals(0, connMgr.numPhysicalConnections());
            connMgr.shutdown();
        }
        int expectTotalBuffers = 0;
        for (FakedPartitionDataStore dataStore : dataStores) {
            expectTotalBuffers += dataStore.totalBuffersToSend.get();
        }
        assertEquals(expectTotalBuffers, buffersCounter.get());
        assertNull(cause.get());
    }

    private static class ReadingThread extends Thread {

        private final RemoteShuffleInputGate gate;

        private final AtomicInteger buffersCounter;

        private final AtomicReference<Throwable> cause;

        ReadingThread(
                RemoteShuffleInputGate gate,
                AtomicInteger buffersCounter,
                AtomicReference<Throwable> cause)
                throws Exception {
            this.gate = gate;
            this.buffersCounter = buffersCounter;
            this.cause = cause;
        }

        @Override
        public void run() {
            try {
                gate.setup();
                while (!gate.isFinished()) {
                    while (true) {
                        Optional<BufferOrEvent> got = gate.pollNext();
                        if (got.isPresent()) {
                            if (got.get().isBuffer()) {
                                got.get().getBuffer().recycleBuffer();
                                buffersCounter.incrementAndGet();
                            } else if (got.get().getEvent() instanceof EndOfPartitionEvent) {
                                break;
                            }
                        } else {
                            Thread.sleep(5);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    gate.close();
                } catch (Throwable t) {
                    cause.set(t);
                }
            }
        }
    }

    private static class FeedingThread extends Thread {

        private final FakedDataPartitionReadingView readingView;
        private final int numBuffersToSend;

        FeedingThread(FakedDataPartitionReadingView readingView, int numBuffersToSend) {
            this.readingView = readingView;
            this.numBuffersToSend = numBuffersToSend;
        }

        @Override
        public void run() {
            TestTransferBufferPool serverBufferPool = new TestTransferBufferPool(20, 64);
            try {
                readingView.notifyBacklog(numBuffersToSend + 1);
                for (int i = 0; i <= numBuffersToSend; i++) {
                    if (readingView.getError() != null) {
                        break;
                    }
                    ByteBuf buffer = serverBufferPool.requestBufferBlocking();
                    if (i == numBuffersToSend) {
                        fillAsEOF(buffer);
                        readingView.setNoMoreData(true);
                    } else {
                        fillAsBuffer(buffer);
                    }
                    readingView.notifyBuffer(buffer);
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                serverBufferPool.destroy();
            }
        }

        private void fillAsBuffer(ByteBuf buffer) {
            BufferUtils.setBufferHeader(buffer, Buffer.DataType.DATA_BUFFER, false, 4);
            buffer.writeInt(0);
        }

        private void fillAsEOF(ByteBuf buffer) throws IOException {
            ByteBuf serialized =
                    EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false).asByteBuf();
            BufferUtils.setBufferHeader(
                    buffer, Buffer.DataType.EVENT_BUFFER, false, serialized.readableBytes());
            buffer.writeBytes(serialized);
        }
    }

    private Pair<ConnectionManager, List<RemoteShuffleInputGate>> createInputGateGroup(
            List<BufferPool> clientBufferPools,
            int numConcurrentReading,
            int upstreamParallelism,
            List<ShuffleWorkerDescriptor> descs)
            throws Exception {
        NettyConfig nettyConfig = new NettyConfig(new Configuration());
        ConnectionManager connMgr =
                ConnectionManager.createReadConnectionManager(nettyConfig, true);
        connMgr.start();
        List<RemoteShuffleInputGate> gates = new ArrayList<>();
        for (BufferPool bufferPool : clientBufferPools) {
            RemoteShuffleInputGate gate =
                    createInputGate(
                            numConcurrentReading, connMgr, upstreamParallelism, bufferPool, descs);
            gates.add(gate);
        }
        return Pair.of(connMgr, gates);
    }

    private RemoteShuffleInputGate createInputGate(
            int numConcurrentReading,
            ConnectionManager connMgr,
            int upstreamParallelism,
            BufferPool bufferPool,
            List<ShuffleWorkerDescriptor> descs)
            throws IOException {
        String taskName = "ReadingIntegrationTest";
        IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();

        InputGateDeploymentDescriptor gateDesc =
                new InputGateDeploymentDescriptor(
                        intermediateDataSetID,
                        ResultPartitionType.BLOCKING,
                        0,
                        createShuffleDescs(upstreamParallelism, descs));
        return new RemoteShuffleInputGate(
                taskName,
                true,
                0,
                Integer.MAX_VALUE,
                gateDesc,
                numConcurrentReading,
                connMgr,
                () -> bufferPool,
                null);
    }

    private ShuffleDescriptor[] createShuffleDescs(
            int upstreamParallelism, List<ShuffleWorkerDescriptor> descs) {
        ShuffleDescriptor[] ret = new ShuffleDescriptor[upstreamParallelism];
        JobID jobID = new JobID(CommonUtils.randomBytes(32));
        for (int i = 0; i < upstreamParallelism; i++) {
            ResultPartitionID rID = new ResultPartitionID();
            int randIdx = random.nextInt(descs.size());
            ShuffleResource shuffleResource =
                    new DefaultShuffleResource(
                            new ShuffleWorkerDescriptor[] {descs.get(randIdx)},
                            DataPartition.DataPartitionType.MAP_PARTITION,
                            DiskType.ANY_TYPE);
            ret[i] = new RemoteShuffleDescriptor(rID, jobID, shuffleResource, true, 1);
        }
        return ret;
    }

    private void startServers(int numShuffleWorkers, int dataScalePerReadingView) throws Exception {
        dataStores = new ArrayList<>();
        nettyServers = new ArrayList<>();
        workerDescs = new ArrayList<>();
        for (int i = 0; i < numShuffleWorkers; i++) {
            int dataPort = AbstractNettyTest.getAvailablePort();
            workerDescs.add(
                    new ShuffleWorkerDescriptor(
                            null, InetAddress.getLocalHost().getHostAddress(), dataPort));
            FakedPartitionDataStore dataStore =
                    new FakedPartitionDataStore(dataScalePerReadingView);
            dataStores.add(dataStore);

            Configuration config = new Configuration();
            config.setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
            config.setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
            NettyConfig nettyConfig = new NettyConfig(config);
            NettyServer server = new NettyServer(dataStore, nettyConfig);
            nettyServers.add(server);
            server.start();
        }
    }

    private class FakedPartitionDataStore extends NoOpPartitionedDataStore {

        private final int dataScalePerReadingView;
        private final AtomicInteger totalBuffersToSend = new AtomicInteger(0);

        FakedPartitionDataStore(int dataScalePerReadingView) {
            this.dataScalePerReadingView = dataScalePerReadingView;
        }

        @Override
        public DataPartitionReadingView createDataPartitionReadingView(ReadingViewContext context) {
            FakedDataPartitionReadingView ret =
                    new FakedDataPartitionReadingView(
                            context.getDataListener(),
                            context.getBacklogListener(),
                            context.getFailureListener());
            final int buffersToSend;
            if (dataScalePerReadingView == 0) {
                buffersToSend = 0;
            } else {
                buffersToSend = dataScalePerReadingView + random.nextInt(dataScalePerReadingView);
            }
            totalBuffersToSend.addAndGet(buffersToSend);
            new FeedingThread(ret, buffersToSend).start();
            return ret;
        }
    }
}
