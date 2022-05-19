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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ProtocolUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.transfer.PartitionSortedBufferTest.DataAndType;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link RemoteShuffleResultPartition}. */
public class RemoteShuffleResultPartitionTest {

    private static final int totalBuffers = 1000;

    private static final int bufferSize = 1024;

    private NetworkBufferPool globalBufferPool;

    private BufferPool sortBufferPool;

    private BufferPool nettyBufferPool;

    private RemoteShuffleResultPartition partitionWriter;

    private FakedRemoteShuffleOutputGate outputGate;

    private BufferCompressor bufferCompressor;

    private BufferDecompressor bufferDecompressor;

    @Before
    public void setup() {
        globalBufferPool = new NetworkBufferPool(totalBuffers, bufferSize);
        bufferCompressor = new BufferCompressor(bufferSize, "LZ4");
        bufferDecompressor = new BufferDecompressor(bufferSize, "LZ4");
    }

    @After
    public void tearDown() throws Exception {
        if (outputGate != null) {
            outputGate.release();
        }

        if (sortBufferPool != null) {
            sortBufferPool.lazyDestroy();
        }
        if (nettyBufferPool != null) {
            nettyBufferPool.lazyDestroy();
        }
        assertEquals(totalBuffers, globalBufferPool.getNumberOfAvailableMemorySegments());
        globalBufferPool.destroy();
    }

    @Test
    public void testWriteNormalRecordWithCompressionEnabled() throws Exception {
        testWriteNormalRecord(true);
    }

    @Test
    public void testWriteNormalRecordWithCompressionDisabled() throws Exception {
        testWriteNormalRecord(false);
    }

    @Test
    public void testWriteLargeRecord() throws Exception {
        int numSubpartitions = 2;
        int numBuffers = 100;
        initResultPartitionWriter(numSubpartitions, 10, 200, false);

        partitionWriter.setup();

        byte[] dataWritten = new byte[bufferSize * numBuffers];
        Random random = new Random();
        random.nextBytes(dataWritten);
        ByteBuffer recordWritten = ByteBuffer.wrap(dataWritten);
        partitionWriter.emitRecord(recordWritten, 0);
        assertEquals(0, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.finish();
        partitionWriter.close();

        List<Buffer> receivedBuffers = outputGate.getReceivedBuffers()[0];

        ByteBuffer recordRead = ByteBuffer.allocate(bufferSize * numBuffers);
        for (Buffer buffer : receivedBuffers) {
            if (buffer.isBuffer()) {
                recordRead.put(
                        buffer.getNioBuffer(
                                BufferUtils.HEADER_LENGTH,
                                buffer.readableBytes() - BufferUtils.HEADER_LENGTH));
            }
        }
        recordWritten.rewind();
        recordRead.flip();
        assertEquals(recordWritten, recordRead);
    }

    @Test
    public void testBroadcastLargeRecord() throws Exception {
        int numSubpartitions = 2;
        int numBuffers = 100;
        initResultPartitionWriter(numSubpartitions, 10, 200, false);

        partitionWriter.setup();

        byte[] dataWritten = new byte[bufferSize * numBuffers];
        Random random = new Random();
        random.nextBytes(dataWritten);
        ByteBuffer recordWritten = ByteBuffer.wrap(dataWritten);
        partitionWriter.broadcastRecord(recordWritten);
        assertEquals(0, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.finish();
        partitionWriter.close();

        ByteBuffer recordRead0 = ByteBuffer.allocate(bufferSize * numBuffers);
        for (Buffer buffer : outputGate.getReceivedBuffers()[0]) {
            if (buffer.isBuffer()) {
                recordRead0.put(
                        buffer.getNioBuffer(
                                BufferUtils.HEADER_LENGTH,
                                buffer.readableBytes() - BufferUtils.HEADER_LENGTH));
            }
        }
        recordWritten.rewind();
        recordRead0.flip();
        assertEquals(recordWritten, recordRead0);

        ByteBuffer recordRead1 = ByteBuffer.allocate(bufferSize * numBuffers);
        for (Buffer buffer : outputGate.getReceivedBuffers()[1]) {
            if (buffer.isBuffer()) {
                recordRead1.put(
                        buffer.getNioBuffer(
                                BufferUtils.HEADER_LENGTH,
                                buffer.readableBytes() - BufferUtils.HEADER_LENGTH));
            }
        }
        recordWritten.rewind();
        recordRead1.flip();
        assertEquals(recordWritten, recordRead0);
    }

    @Test
    public void testFlush() throws Exception {
        int numSubpartitions = 10;

        initResultPartitionWriter(numSubpartitions, 10, 20, false);
        partitionWriter.setup();

        partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 0);
        partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 1);
        assertEquals(3, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.broadcastRecord(ByteBuffer.allocate(bufferSize));
        assertEquals(2, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.flush(0);
        assertEquals(0, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 2);
        partitionWriter.emitRecord(ByteBuffer.allocate(bufferSize), 3);
        assertEquals(3, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.flushAll();
        assertEquals(0, sortBufferPool.bestEffortGetNumOfUsedBuffers());

        partitionWriter.finish();
        partitionWriter.close();
    }

    private void testWriteNormalRecord(boolean compressionEnabled) throws Exception {
        int numSubpartitions = 4;
        int numRecords = 100;
        Random random = new Random();

        initResultPartitionWriter(numSubpartitions, 100, 500, compressionEnabled);
        partitionWriter.setup();
        assertTrue(outputGate.isSetup());

        Queue<DataAndType>[] dataWritten = new Queue[numSubpartitions];
        IntStream.range(0, numSubpartitions).forEach(i -> dataWritten[i] = new ArrayDeque<>());
        int[] numBytesWritten = new int[numSubpartitions];
        Arrays.fill(numBytesWritten, 0);

        for (int i = 0; i < numRecords; i++) {
            byte[] data = new byte[random.nextInt(2 * bufferSize) + 1];
            if (compressionEnabled) {
                byte randomByte = (byte) random.nextInt();
                Arrays.fill(data, randomByte);
            } else {
                random.nextBytes(data);
            }
            ByteBuffer record = ByteBuffer.wrap(data);
            boolean isBroadCast = random.nextBoolean();

            if (isBroadCast) {
                partitionWriter.broadcastRecord(record);
                IntStream.range(0, numSubpartitions)
                        .forEach(
                                subpartition ->
                                        recordDataWritten(
                                                record,
                                                DataType.DATA_BUFFER,
                                                subpartition,
                                                dataWritten,
                                                numBytesWritten));
            } else {
                int subpartition = random.nextInt(numSubpartitions);
                partitionWriter.emitRecord(record, subpartition);
                recordDataWritten(
                        record, DataType.DATA_BUFFER, subpartition, dataWritten, numBytesWritten);
            }
        }

        partitionWriter.finish();
        assertTrue(outputGate.isFinished());
        partitionWriter.close();
        assertTrue(outputGate.isClosed());

        for (int subpartition = 0; subpartition < numSubpartitions; ++subpartition) {
            ByteBuffer record = EventSerializer.toSerializedEvent(EndOfPartitionEvent.INSTANCE);
            recordDataWritten(
                    record, DataType.EVENT_BUFFER, subpartition, dataWritten, numBytesWritten);
        }

        outputGate
                .getFinishedRegions()
                .forEach(
                        regionIndex ->
                                assertTrue(
                                        outputGate
                                                .getNumBuffersByRegion()
                                                .containsKey(regionIndex)));

        int[] numBytesRead = new int[numSubpartitions];
        List<Buffer>[] receivedBuffers = outputGate.getReceivedBuffers();
        List<Buffer>[] validateTarget = new List[numSubpartitions];
        Arrays.fill(numBytesRead, 0);
        for (int i = 0; i < numSubpartitions; i++) {
            validateTarget[i] = new ArrayList<>();
            for (Buffer buffer : receivedBuffers[i]) {
                for (Buffer unpackedBuffer : BufferPacker.unpack(buffer.asByteBuf())) {
                    if (compressionEnabled && unpackedBuffer.isCompressed()) {
                        Buffer decompressedBuffer =
                                bufferDecompressor.decompressToIntermediateBuffer(unpackedBuffer);
                        ByteBuffer decompressed = decompressedBuffer.getNioBufferReadable();
                        int numBytes = decompressed.remaining();
                        MemorySegment segment =
                                MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                        segment.put(0, decompressed, numBytes);
                        decompressedBuffer.recycleBuffer();
                        validateTarget[i].add(
                                new NetworkBuffer(
                                        segment,
                                        buf -> {},
                                        unpackedBuffer.getDataType(),
                                        numBytes));
                        numBytesRead[i] += numBytes;
                    } else {
                        numBytesRead[i] += buffer.readableBytes();
                        validateTarget[i].add(buffer);
                    }
                }
            }
        }
        IntStream.range(0, numSubpartitions).forEach(subpartitions -> {});
        PartitionSortedBufferTest.checkWriteReadResult(
                numSubpartitions, numBytesWritten, numBytesWritten, dataWritten, validateTarget);
    }

    private void initResultPartitionWriter(
            int numSubpartitions,
            int sortBufferPoolSize,
            int nettyBufferPoolSize,
            boolean compressionEnabled)
            throws Exception {

        sortBufferPool = globalBufferPool.createBufferPool(sortBufferPoolSize, sortBufferPoolSize);
        nettyBufferPool =
                globalBufferPool.createBufferPool(nettyBufferPoolSize, nettyBufferPoolSize);

        outputGate =
                new FakedRemoteShuffleOutputGate(
                        getShuffleDescriptor(), numSubpartitions, () -> nettyBufferPool);
        outputGate.setup();

        if (compressionEnabled) {
            partitionWriter =
                    new RemoteShuffleResultPartition(
                            "RemoteShuffleResultPartitionWriterTest",
                            0,
                            new ResultPartitionID(),
                            ResultPartitionType.BLOCKING,
                            numSubpartitions,
                            numSubpartitions,
                            bufferSize,
                            new ResultPartitionManager(),
                            bufferCompressor,
                            () -> sortBufferPool,
                            outputGate);
        } else {
            partitionWriter =
                    new RemoteShuffleResultPartition(
                            "RemoteShuffleResultPartitionWriterTest",
                            0,
                            new ResultPartitionID(),
                            ResultPartitionType.BLOCKING,
                            numSubpartitions,
                            numSubpartitions,
                            bufferSize,
                            new ResultPartitionManager(),
                            null,
                            () -> sortBufferPool,
                            outputGate);
        }
    }

    private void recordDataWritten(
            ByteBuffer record,
            DataType dataType,
            int subpartition,
            Queue<DataAndType>[] dataWritten,
            int[] numBytesWritten) {

        record.rewind();
        dataWritten[subpartition].add(new DataAndType(record, dataType));
        numBytesWritten[subpartition] += record.remaining();
    }

    private static class FakedRemoteShuffleOutputGate extends RemoteShuffleOutputGate {

        private boolean isSetup;
        private boolean isFinished;
        private boolean isClosed;
        private final List<Buffer>[] receivedBuffers;
        private final Map<Integer, Integer> numBuffersByRegion;
        private final Set<Integer> finishedRegions;
        private int currentRegionIndex;
        private boolean currentIsBroadcast;

        FakedRemoteShuffleOutputGate(
                RemoteShuffleDescriptor shuffleDescriptor,
                int numSubpartitions,
                SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

            super(
                    shuffleDescriptor,
                    numSubpartitions,
                    bufferSize,
                    ProtocolUtils.emptyDataPartitionType(),
                    bufferPoolFactory,
                    new ConnectionManager(null, null, 3, Duration.ofMillis(1)));
            isSetup = false;
            isFinished = false;
            isClosed = false;
            numBuffersByRegion = new HashMap<>();
            finishedRegions = new HashSet<>();
            currentRegionIndex = -1;
            receivedBuffers = new ArrayList[numSubpartitions];
            IntStream.range(0, numSubpartitions)
                    .forEach(i -> receivedBuffers[i] = new ArrayList<>());
            currentIsBroadcast = false;
        }

        @Override
        public void setup() throws IOException, InterruptedException {
            bufferPool = bufferPoolFactory.get();
            isSetup = true;
        }

        @Override
        public void write(Buffer buffer, int subIdx) {
            if (currentIsBroadcast) {
                assertEquals(0, subIdx);
                ByteBuffer byteBuffer = buffer.getNioBufferReadable();
                for (int i = 0; i < numSubs; i++) {
                    int numBytes = buffer.readableBytes();
                    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(numBytes);
                    byteBuffer.rewind();
                    segment.put(0, byteBuffer, numBytes);
                    receivedBuffers[i].add(
                            new NetworkBuffer(
                                    segment,
                                    buf -> {},
                                    buffer.getDataType(),
                                    buffer.isCompressed(),
                                    numBytes));
                }
                buffer.recycleBuffer();
            } else {
                receivedBuffers[subIdx].add(buffer);
            }
            if (numBuffersByRegion.containsKey(currentRegionIndex)) {
                int prev = numBuffersByRegion.get(currentRegionIndex);
                numBuffersByRegion.put(currentRegionIndex, prev + 1);
            } else {
                numBuffersByRegion.put(currentRegionIndex, 1);
            }
        }

        @Override
        public void regionStart(boolean isBroadcast) {
            currentIsBroadcast = isBroadcast;
            currentRegionIndex++;
        }

        @Override
        public void regionFinish() {
            if (finishedRegions.contains(currentRegionIndex)) {
                throw new IllegalStateException("Unexpected region: " + currentRegionIndex);
            }
            finishedRegions.add(currentRegionIndex);
        }

        @Override
        public void finish() throws InterruptedException {
            isFinished = true;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        public List<Buffer>[] getReceivedBuffers() {
            return receivedBuffers;
        }

        public Map<Integer, Integer> getNumBuffersByRegion() {
            return numBuffersByRegion;
        }

        public Set<Integer> getFinishedRegions() {
            return finishedRegions;
        }

        public boolean isSetup() {
            return isSetup;
        }

        public boolean isFinished() {
            return isFinished;
        }

        public boolean isClosed() {
            return isClosed;
        }

        public void release() throws Exception {
            IntStream.range(0, numSubs)
                    .forEach(
                            subpartitionIndex -> {
                                receivedBuffers[subpartitionIndex].forEach(Buffer::recycleBuffer);
                                receivedBuffers[subpartitionIndex].clear();
                            });
            numBuffersByRegion.clear();
            finishedRegions.clear();
            super.close();
        }
    }

    private RemoteShuffleDescriptor getShuffleDescriptor() throws Exception {
        return new RemoteShuffleDescriptor(
                new ResultPartitionID(),
                new JobID(CommonUtils.randomBytes(64)),
                new DefaultShuffleResource(
                        new ShuffleWorkerDescriptor[] {
                            new ShuffleWorkerDescriptor(null, "localhost", 0)
                        },
                        DataPartition.DataPartitionType.MAP_PARTITION));
    }
}
