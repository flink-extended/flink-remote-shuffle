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

package com.alibaba.flink.shuffle.plugin.transfer;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.exception.PartitionNotFoundException;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ShuffleReadClient;
import com.alibaba.flink.shuffle.transfer.TransferBufferPool;

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for {@link RemoteShuffleInputGate}. */
public class RemoteShuffleInputGateTest {

    private static final int bufferSize = 1;

    private NetworkBufferPool networkBufferPool;

    @Before
    public void setup() {
        networkBufferPool =
                new NetworkBufferPool(
                        RemoteShuffleInputGateFactory.MIN_BUFFERS_PER_GATE, bufferSize);
    }

    @After
    public void teatDown() {
        networkBufferPool.destroy();
    }

    @Test
    public void testOpenChannelsOneByOne() throws Exception {
        RemoteShuffleInputGate gate = createRemoteShuffleInputGate(2, 1);

        gate.setup();
        assertTrue(getChannel(gate, 0).isConnected);
        assertTrue(getChannel(gate, 1).isConnected);
        ReadingThread readingThread = new ReadingThread(gate);
        readingThread.start();
        assertEquals(2, gate.getShuffleReadClients().size());
        assertEquals(2, gate.getNumberOfInputChannels());
        check(() -> assertEquals(Thread.State.WAITING, readingThread.getState()));
        assertEquals(0, readingThread.numRead);
        assertTrue(getChannel(gate, 0).isOpened);
        assertFalse(getChannel(gate, 1).isOpened);

        getChannel(gate, 0).triggerEndOfPartitionEvent();
        readingThread.kick();
        check(() -> assertEquals(Thread.State.WAITING, readingThread.getState()));
        assertEquals(1, readingThread.numRead);
        assertTrue(getChannel(gate, 0).isClosed);
        assertTrue(getChannel(gate, 1).isOpened);

        getChannel(gate, 1).triggerEndOfPartitionEvent();
        readingThread.kick();
        check(() -> assertSame(readingThread.getState(), Thread.State.TERMINATED));
        assertEquals(2, readingThread.numRead);
        assertTrue(getChannel(gate, 1).isClosed);
    }

    @Test
    public void testBasicRoutine() throws Exception {
        RemoteShuffleInputGate gate = createRemoteShuffleInputGate(1, 1);
        gate.setup();
        ReadingThread readingThread = new ReadingThread(gate);
        readingThread.start();
        check(() -> assertSame(readingThread.getState(), Thread.State.WAITING));
        assertEquals(0, readingThread.numRead);
        assertFalse(gate.isAvailable());

        getChannel(gate, 0).triggerData();
        assertTrue(gate.isAvailable());
        readingThread.kick();
        check(() -> assertSame(readingThread.getState(), Thread.State.WAITING));
        assertEquals(1, readingThread.numRead);
        assertFalse(gate.isAvailable());

        getChannel(gate, 0).triggerEndOfPartitionEvent();
        readingThread.kick();
        check(() -> assertSame(readingThread.getState(), Thread.State.TERMINATED));
        assertEquals(2, readingThread.numRead);

        assertNull(readingThread.cause);
    }

    @Test
    public void testReadingFailure() throws Exception {
        RemoteShuffleInputGate gate = createRemoteShuffleInputGate(1, 1);
        gate.setup();
        ReadingThread readingThread = new ReadingThread(gate);
        readingThread.start();
        check(() -> assertSame(readingThread.getState(), Thread.State.WAITING));
        assertEquals(0, readingThread.numRead);

        getChannel(gate, 0).triggerFailure();
        readingThread.kick();
        check(() -> assertSame(readingThread.getState(), Thread.State.TERMINATED));

        assertNotNull(readingThread.cause);
    }

    @Test
    public void testClosing() throws Exception {
        RemoteShuffleInputGate gate = createRemoteShuffleInputGate(1, 1);
        gate.setup();
        gate.close();
        assertTrue(getChannel(gate, 0).isClosed);
    }

    @Test
    public void testFireHandshakeByPollNext() throws Exception {
        RemoteShuffleInputGate gate = createRemoteShuffleInputGate(1, 1);
        gate.setup();
        assertTrue(gate.isAvailable());
        assertTrue(getChannel(gate, 0).isConnected);
        assertFalse(getChannel(gate, 0).isOpened);

        assertFalse(gate.pollNext().isPresent());
        assertFalse(gate.isAvailable());
        assertTrue(getChannel(gate, 0).isOpened);

        getChannel(gate, 0).triggerData();
        assertTrue(gate.isAvailable());
        Optional<BufferOrEvent> polled = gate.pollNext();
        assertTrue(polled.isPresent());
        polled.get().getBuffer().recycleBuffer();
        assertFalse(gate.pollNext().isPresent());
        assertFalse(gate.isAvailable());

        gate.close();
    }

    @Test
    public void testPartitionException() throws Exception {
        final RemoteShuffleInputGate gate0 = createRemoteShuffleInputGate(1, 1, true);
        assertThrows(ShuffleException.class, gate0::setup);
        gate0.close();

        final RemoteShuffleInputGate gate1 = createRemoteShuffleInputGate(1, 1);
        gate1.setup();
        ReadingThread readingThread = new ReadingThread(gate1);
        readingThread.start();
        check(() -> assertEquals(readingThread.getState(), Thread.State.WAITING));
        assertEquals(0, readingThread.numRead);

        getChannel(gate1, 0).triggerPartitionNotFound();
        readingThread.kick();
        check(() -> assertEquals(readingThread.getState(), Thread.State.TERMINATED));

        Class<?> clazz = com.alibaba.flink.shuffle.plugin.transfer.PartitionNotFoundException.class;
        assertEquals(clazz, readingThread.cause.getClass());
        gate1.close();
    }

    private RemoteShuffleInputGate createRemoteShuffleInputGate(
            int numShuffleDescs, int numConcurrentReading) throws Exception {
        return createRemoteShuffleInputGate(numShuffleDescs, numConcurrentReading, false);
    }

    private RemoteShuffleInputGate createRemoteShuffleInputGate(
            int numShuffleDescs, int numConcurrentReading, boolean throwsWhenConnect)
            throws Exception {
        return new TestingRemoteShuffleInputGate(
                numConcurrentReading,
                createGateDescriptor(numShuffleDescs),
                () ->
                        networkBufferPool.createBufferPool(
                                RemoteShuffleInputGateFactory.MIN_BUFFERS_PER_GATE,
                                RemoteShuffleInputGateFactory.MIN_BUFFERS_PER_GATE),
                throwsWhenConnect);
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

    private FakedShuffleReadClient getChannel(RemoteShuffleInputGate gate, int idx) {
        return (FakedShuffleReadClient) gate.getShuffleReadClients().get(idx);
    }

    protected void check(Runnable runnable) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            try {
                runnable.run();
                return;
            } catch (Throwable t) {
                Thread.sleep(200);
            }
        }
        fail();
    }

    private class TestingRemoteShuffleInputGate extends RemoteShuffleInputGate {

        private final boolean throwsWhenConnect;

        public TestingRemoteShuffleInputGate(
                int numConcurrentReading,
                InputGateDeploymentDescriptor gateDescriptor,
                SupplierWithException<BufferPool, IOException> bufferPoolFactory,
                boolean throwsWhenConnect) {

            super(
                    "RemoteShuffleInputGateTest",
                    true,
                    0,
                    bufferSize,
                    gateDescriptor,
                    numConcurrentReading,
                    null,
                    bufferPoolFactory,
                    null);
            this.throwsWhenConnect = throwsWhenConnect;
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

            List<ByteBuf> buffers = new ArrayList<>();
            for (int i = 0; i < 1024; i++) {
                NetworkBuffer buffer =
                        new NetworkBuffer(
                                MemorySegmentFactory.allocateUnpooledSegment(1024),
                                FreeingBufferRecycler.INSTANCE);
                buffers.add(buffer.asByteBuf());
            }
            bufferPool.addBuffers(buffers);

            return new FakedShuffleReadClient(this, bufferPool, dataListener, failureListener);
        }
    }

    private class FakedShuffleReadClient extends ShuffleReadClient {

        private final TestingRemoteShuffleInputGate parent;
        private final TransferBufferPool bufferPool;
        private final Consumer<ByteBuf> dataListener;
        private final Consumer<Throwable> failureListener;

        private boolean isConnected;
        private boolean isOpened;
        private boolean isClosed;

        public FakedShuffleReadClient(
                TestingRemoteShuffleInputGate parent,
                TransferBufferPool bufferPool,
                Consumer<ByteBuf> dataListener,
                Consumer<Throwable> failureListener) {
            super(
                    new InetSocketAddress(1),
                    new DataSetID(CommonUtils.randomBytes(1)),
                    new MapPartitionID(CommonUtils.randomBytes(1)),
                    0,
                    0,
                    Integer.MAX_VALUE,
                    bufferPool,
                    new ConnectionManager(null, null, 3, Duration.ofMillis(1)),
                    dataListener,
                    failureListener);
            this.bufferPool = bufferPool;
            this.parent = parent;
            this.dataListener = dataListener;
            this.failureListener = failureListener;
        }

        @Override
        public void connect() {
            if (parent.throwsWhenConnect) {
                throw new ShuffleException("Connect failure.");
            }
            isConnected = true;
        }

        @Override
        public void open() {
            isOpened = true;
        }

        @Override
        public boolean isOpened() {
            return isOpened;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        public void triggerData() throws Exception {
            ByteBuf byteBuf =
                    new NetworkBuffer(
                            MemorySegmentFactory.allocateUnpooledSegment(1024),
                            FreeingBufferRecycler.INSTANCE);
            BufferUtils.setBufferHeader(byteBuf, Buffer.DataType.DATA_BUFFER, false, 1);
            byteBuf.writeByte(2);
            dataListener.accept(byteBuf);
        }

        public void triggerData(ByteBuffer data) throws Exception {
            ByteBuf byteBuf =
                    new NetworkBuffer(
                            MemorySegmentFactory.allocateUnpooledSegment(1024),
                            FreeingBufferRecycler.INSTANCE);
            BufferUtils.setBufferHeader(
                    byteBuf, Buffer.DataType.DATA_BUFFER, false, data.remaining());
            byteBuf.writeBytes(data);
            data.position(0);
            dataListener.accept(byteBuf);
        }

        public void triggerEndOfPartitionEvent() throws IOException {
            Buffer buffer = EventSerializer.toBuffer(EndOfPartitionEvent.INSTANCE, false);
            ByteBuf byteBuf =
                    new NetworkBuffer(
                            MemorySegmentFactory.allocateUnpooledSegment(1024),
                            FreeingBufferRecycler.INSTANCE);
            BufferUtils.setBufferHeader(
                    byteBuf, buffer.getDataType(), buffer.isCompressed(), buffer.readableBytes());
            dataListener.accept(byteBuf);
        }

        public void triggerFailure() {
            failureListener.accept(new Exception(""));
        }

        public void triggerPartitionNotFound() {
            failureListener.accept(new IOException(PartitionNotFoundException.class.getName()));
        }

        @Override
        public void notifyAvailableCredits(int numCredits) {}
    }

    private static class ReadingThread extends Thread {

        private final Object lock = new Object();
        private final RemoteShuffleInputGate gate;
        private int numRead;
        private Throwable cause;

        private final List<ByteBuffer> buffers = new ArrayList<>();

        public ReadingThread(RemoteShuffleInputGate gate) {
            this.gate = gate;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Optional<BufferOrEvent> got = gate.pollNext();
                    if (got.isPresent()) {
                        numRead++;
                        if (got.get().isBuffer()) {
                            buffers.add(got.get().getBuffer().getNioBufferReadable());
                            got.get().getBuffer().recycleBuffer();
                        } else if (gate.isFinished() && got.get().moreAvailable()) {
                            throw new Exception("Got EOF but indicating more data available.");
                        }
                    } else {
                        if (gate.isFinished()) {
                            break;
                        } else {
                            synchronized (lock) {
                                lock.wait();
                            }
                        }
                    }
                } catch (Throwable t) {
                    cause = t;
                    break;
                }
            }
        }

        public void kick() {
            synchronized (lock) {
                lock.notify();
            }
        }

        public List<ByteBuffer> getBuffers() {
            return buffers;
        }
    }
}
