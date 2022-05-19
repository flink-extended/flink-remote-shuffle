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
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.transfer.AbstractNettyTest;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.FakedDataPartitionWritingView;
import com.alibaba.flink.shuffle.transfer.NettyConfig;
import com.alibaba.flink.shuffle.transfer.NettyServer;
import com.alibaba.flink.shuffle.transfer.TestTransferBufferPool;
import com.alibaba.flink.shuffle.transfer.TransferBufferPool;
import com.alibaba.flink.shuffle.transfer.utils.NoOpPartitionedDataStore;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyBufferSize;
import static com.alibaba.flink.shuffle.common.utils.ProtocolUtils.emptyDataPartitionType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Integration test for writing process. */
@RunWith(Parameterized.class)
public class WritingIntegrationTest {

    private static final Logger LOG = LoggerFactory.getLogger(WritingIntegrationTest.class);

    private String localhost;

    private TransferBufferPool serverBufferPool;

    private NetworkBufferPool networkBufferPool;

    private FakedPartitionDataStore dataStore;

    private NettyConfig nettyConfig;

    private NettyServer nettyServer;

    private int numSubs;

    private Random random;

    private final int parallelism;

    // Num of output gates share a ConnectionManager.
    private final int groupSize;

    // Num of buffers for transmitting per writing channel.
    private final int nBuffersPerTask;

    private final int dataScale;

    public WritingIntegrationTest(
            int parallelism, int groupSize, int nBuffersPerTask, int dataScale) {
        this.parallelism = parallelism;
        this.groupSize = groupSize;
        this.nBuffersPerTask = nBuffersPerTask;
        this.dataScale = dataScale;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {10, 1, 10, 200},
                    {10, 5, 10, 200},
                    {10, 10, 10, 200},
                    {10, 10, 10, 0},
                    {1200, 20, 10, 200},
                    {500, 20, 40, 200},
                });
    }

    @Before
    public void setup() throws Exception {
        nettyConfig = new NettyConfig(new Configuration());
        int dataPort = AbstractNettyTest.getAvailablePort();
        nettyConfig.getConfig().setInteger(TransferOptions.SERVER_DATA_PORT, dataPort);
        if (localhost == null) {
            localhost = InetAddress.getLocalHost().getHostAddress();
        }

        random = new Random();

        // Setup server.
        serverBufferPool = new TestTransferBufferPool(2000, 64);
        dataStore = new FakedPartitionDataStore(() -> (Buffer) (serverBufferPool.requestBuffer()));
        nettyServer = new NettyServer(dataStore, nettyConfig);
        nettyServer.disableHeartbeat();
        nettyServer.start();

        // Setup client.
        numSubs = 32;
        networkBufferPool = new NetworkBufferPool(20000, 32);
    }

    @After
    public void tearDown() {
        assertEquals(2000, serverBufferPool.numBuffers());
        assertEquals(20000, networkBufferPool.getNumberOfAvailableMemorySegments());
        nettyServer.shutdown();
        serverBufferPool.destroy();
        networkBufferPool.destroy();
    }

    @Test(timeout = 300_000)
    public void test() throws Exception {

        List<BufferPool> transBufferPools = new ArrayList<>(parallelism);

        for (int i = 0; i < parallelism; i++) {
            transBufferPools.add(
                    networkBufferPool.createBufferPool(nBuffersPerTask, nBuffersPerTask));
        }
        List<ConnectionManager> connMgrs = new ArrayList<>();
        List<RemoteShuffleOutputGate> outputGates = new ArrayList<>();
        for (int i = 0; i < parallelism; i += groupSize) {
            Pair<ConnectionManager, List<RemoteShuffleOutputGate>> pair =
                    createOutputGateGroup(transBufferPools.subList(i, i + groupSize));
            connMgrs.add(pair.getLeft());
            outputGates.addAll(pair.getRight());
        }

        List<Thread> threads = new ArrayList<>();
        int totalNumBuffersToSend = 0;
        AtomicReference<Throwable> cause = new AtomicReference<>(null);
        for (int i = 0; i < parallelism; i++) {
            RemoteShuffleOutputGate gate = outputGates.get(i);
            gate.setup();
            final int numBuffersToSend;
            if (dataScale == 0) {
                numBuffersToSend = 0;
            } else {
                numBuffersToSend = dataScale + random.nextInt(dataScale);
            }
            totalNumBuffersToSend += numBuffersToSend;
            if (i % groupSize == 1) {
                threads.add(new WritingThread(gate, numBuffersToSend, true, cause));
            } else {
                threads.add(new WritingThread(gate, numBuffersToSend, false, cause));
            }
        }
        threads.forEach(t -> t.start());
        for (Thread t : threads) {
            t.join();
        }
        assertNull(cause.get());

        for (FakedDataPartitionWritingView writingView : dataStore.writingViews) {
            checkUntil(
                    () -> assertTrue(writingView.isFinished() || writingView.getError() != null));
        }
        assertEquals(totalNumBuffersToSend, dataStore.numReceivedBuffers.get());
        for (ConnectionManager connMgr : connMgrs) {
            assertEquals(0, connMgr.numPhysicalConnections());
            connMgr.shutdown();
        }
        transBufferPools.forEach(pool -> pool.lazyDestroy());
    }

    private class WritingThread extends Thread {

        RemoteShuffleOutputGate gate;

        int numBuffers;

        boolean throwsWhenWriting;

        AtomicReference<Throwable> cause;

        WritingThread(
                RemoteShuffleOutputGate gate,
                int numBuffers,
                boolean throwsWhenWriting,
                AtomicReference<Throwable> cause) {
            this.gate = gate;
            this.numBuffers = numBuffers;
            this.throwsWhenWriting = throwsWhenWriting;
            this.cause = cause;
        }

        @Override
        public void run() {
            try {
                int regionSize = 10;
                for (int i = 0; i < numBuffers; i++) {
                    MemorySegment mem = gate.getBufferPool().requestMemorySegmentBlocking();
                    NetworkBuffer buffer = new NetworkBuffer(mem, gate.getBufferPool()::recycle);
                    while (buffer.readableBytes() + 4 <= buffer.capacity()) {
                        buffer.writeByte(random.nextInt());
                    }
                    if (i % regionSize == 0) {
                        // No need to test broadcast which doesn't have effect on credit-based
                        // transportation.
                        gate.regionStart(false);
                    }
                    gate.write(buffer, random.nextInt(numSubs));
                    if (i % regionSize == regionSize - 1) {
                        gate.regionFinish();
                    }
                }
                if (numBuffers % regionSize != 0) {
                    gate.regionFinish();
                }
                if (throwsWhenWriting) {
                    throw new Exception("Manual exception.");
                }
                gate.finish();
            } catch (Throwable t) {
            } finally {
                try {
                    gate.close();
                } catch (Throwable t) {
                    cause.set(t);
                }
            }
        }
    }

    private Pair<ConnectionManager, List<RemoteShuffleOutputGate>> createOutputGateGroup(
            List<BufferPool> clientBufferPools) throws Exception {
        ConnectionManager connMgr =
                ConnectionManager.createWriteConnectionManager(nettyConfig, true);
        connMgr.start();

        List<RemoteShuffleOutputGate> gates = new ArrayList<>();
        for (BufferPool bp : clientBufferPools) {
            gates.add(createOutputGate(connMgr, bp));
        }
        return Pair.of(connMgr, gates);
    }

    private RemoteShuffleOutputGate createOutputGate(
            ConnectionManager connManager, BufferPool bufferPool) throws Exception {
        ResultPartitionID resultPartitionID = new ResultPartitionID();
        JobID jobID = new JobID(CommonUtils.randomBytes(32));
        ShuffleResource shuffleResource =
                new DefaultShuffleResource(
                        new ShuffleWorkerDescriptor[] {
                            new ShuffleWorkerDescriptor(
                                    null, localhost, nettyConfig.getServerPort())
                        },
                        DataPartition.DataPartitionType.MAP_PARTITION);
        RemoteShuffleDescriptor shuffleDescriptor =
                new RemoteShuffleDescriptor(resultPartitionID, jobID, shuffleResource);
        return new RemoteShuffleOutputGate(
                shuffleDescriptor,
                numSubs,
                emptyBufferSize(),
                emptyDataPartitionType(),
                () -> bufferPool,
                connManager);
    }

    protected void checkUntil(Runnable runnable) throws InterruptedException {
        Throwable lastThrowable = null;
        for (int i = 0; i < 100; i++) {
            try {
                runnable.run();
                return;
            } catch (Throwable t) {
                lastThrowable = t;
                Thread.sleep(200);
            }
        }
        LOG.info("", lastThrowable);
        assertTrue(false);
    }

    private static class FakedPartitionDataStore extends NoOpPartitionedDataStore {

        private final Supplier<Buffer> bufferSupplier;
        private final AtomicInteger numReceivedBuffers;
        private final List<FakedDataPartitionWritingView> writingViews;

        FakedPartitionDataStore(Supplier<Buffer> bufferSupplier) {
            this.bufferSupplier = bufferSupplier;
            this.numReceivedBuffers = new AtomicInteger(0);
            this.writingViews = new CopyOnWriteArrayList<>();
        }

        @Override
        public DataPartitionWritingView createDataPartitionWritingView(WritingViewContext context) {
            List<Buffer> buffers = new ArrayList<>();
            buffers.add(bufferSupplier.get());
            FakedDataPartitionWritingView writingView =
                    new FakedDataPartitionWritingView(
                            context.getDataSetID(),
                            context.getMapPartitionID(),
                            ignore -> numReceivedBuffers.addAndGet(1),
                            context.getDataRegionCreditListener(),
                            context.getFailureListener(),
                            buffers);
            writingViews.add(writingView);
            return writingView;
        }
    }
}
