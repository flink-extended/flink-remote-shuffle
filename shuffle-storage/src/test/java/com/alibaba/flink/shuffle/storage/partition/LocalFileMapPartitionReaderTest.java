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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;
import com.alibaba.flink.shuffle.storage.utils.TestDataListener;
import com.alibaba.flink.shuffle.storage.utils.TestFailureListener;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalFileMapPartitionReader}. */
@RunWith(Parameterized.class)
public class LocalFileMapPartitionReaderTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalFileMapPartitionReaderTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testReadData() throws Throwable {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                false,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 1);
        assertEquals(numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadEmptyData() throws Throwable {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                0,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                0,
                false,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 1);
        assertEquals(0, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadWithEmptyReducePartition() throws Throwable {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                true,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 1);
        assertEquals(
                numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS / 2, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadMultipleReducePartitions() throws Throwable {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                false,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 3);
        assertEquals(numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadMultipleReducePartitionsWithEmptyOnes() throws Throwable {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                true,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 3);
        assertEquals(
                numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS / 2, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testRelease() throws Throwable {
        testReleaseOrOnError(true);
    }

    @Test
    public void testOnError() throws Throwable {
        testReleaseOrOnError(false);
    }

    @Test
    public void testCompatibleWithStorageV0() throws Throwable {
        try (FileInputStream fis =
                        new FileInputStream(
                                "src/test/resources/data_for_storage_compatibility_test/storageV0.meta");
                DataInputStream dis = new DataInputStream(fis)) {
            LocalMapPartitionFileMeta fileMeta = LocalMapPartitionFileMeta.readFrom(dis);
            LocalMapPartitionFile partitionFile =
                    new LocalMapPartitionFile(fileMeta, Integer.MAX_VALUE, false);
            int buffersRead = readData(partitionFile, 1);
            assertEquals(10 * 10 * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        }
    }

    private void testReleaseOrOnError(boolean isRelease) throws Throwable {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                10,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                100,
                false,
                dataChecksumEnabled);

        TestDataListener dataListener = new TestDataListener();
        TestFailureListener failureListener = new TestFailureListener();
        LocalFileMapPartitionReader partitionReader =
                createPartitionReader(0, 0, dataListener, failureListener, partitionFile);

        BufferQueue bufferQueue = createBufferQueue(20);
        partitionReader.readData(bufferQueue, bufferQueue::add);
        assertNotNull(dataListener.waitData(0));

        BufferWithBacklog buffer = partitionReader.nextBuffer();
        assertNotNull(buffer);
        assertTrue(buffer.getBacklog() > 0);
        BufferUtils.recycleBuffer(buffer.getBuffer());

        if (isRelease) {
            partitionReader.release(new ShuffleException("Test exception."));
            assertTrue(failureListener.isFailed());
        } else {
            partitionReader.onError(new ShuffleException("Test exception."));
            assertFalse(failureListener.isFailed());
        }

        assertNull(partitionReader.nextBuffer());
        assertThrows(
                Exception.class, () -> partitionReader.readData(bufferQueue, bufferQueue::add));
        assertEquals(20, bufferQueue.size());

        if (!isRelease) {
            partitionReader.release(new ShuffleException("Test exception."));
            assertFalse(failureListener.isFailed());
        }
    }

    private LocalMapPartitionFile createPartitionFile() {
        return StorageTestUtils.createLocalMapPartitionFile(
                temporaryFolder.getRoot().getAbsolutePath());
    }

    private int readData(LocalMapPartitionFile partitionFile, int numPartitions) throws Throwable {
        Map<LocalFileMapPartitionReader, TestDataListener> readers = new ConcurrentHashMap<>();
        for (int partitionIndex = 0; partitionIndex < StorageTestUtils.NUM_REDUCE_PARTITIONS; ) {
            TestDataListener dataListener = new TestDataListener();
            readers.put(
                    createPartitionReader(
                            partitionIndex,
                            Math.min(
                                    partitionIndex + numPartitions - 1,
                                    StorageTestUtils.NUM_REDUCE_PARTITIONS - 1),
                            dataListener,
                            partitionFile),
                    dataListener);
            partitionIndex += numPartitions;
        }

        int buffersRead = 0;
        BufferQueue bufferQueue = createBufferQueue(10);

        while (!readers.isEmpty()) {
            for (LocalFileMapPartitionReader reader : readers.keySet()) {
                boolean hasRemaining = reader.readData(bufferQueue, bufferQueue::add);
                TestDataListener dataListener = readers.get(reader);

                if (!hasRemaining) {
                    readers.remove(reader);
                }
                assertNotNull(dataListener.waitData(0));

                BufferWithBacklog buffer;
                while ((buffer = reader.nextBuffer()) != null) {
                    ++buffersRead;
                    buffer.getBuffer().release();
                }

                if (!hasRemaining) {
                    assertTrue(reader.isFinished());
                }
            }
        }

        assertEquals(10, bufferQueue.size());
        return buffersRead;
    }

    private BufferQueue createBufferQueue(int numBuffers) {
        List<ByteBuffer> buffers = new ArrayList<>();
        for (int i = 0; i < numBuffers; ++i) {
            buffers.add(CommonUtils.allocateDirectByteBuffer(StorageTestUtils.DATA_BUFFER_SIZE));
        }
        return new BufferQueue(buffers);
    }

    private LocalFileMapPartitionReader createPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            LocalMapPartitionFile partitionFile)
            throws Exception {
        return createPartitionReader(
                startPartitionIndex,
                endPartitionIndex,
                dataListener,
                StorageTestUtils.NO_OP_FAILURE_LISTENER,
                partitionFile);
    }

    private LocalFileMapPartitionReader createPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            FailureListener failureListener,
            LocalMapPartitionFile partitionFile)
            throws Exception {
        LocalMapPartitionFileReader fileReader =
                new LocalMapPartitionFileReader(
                        dataChecksumEnabled, startPartitionIndex, endPartitionIndex, partitionFile);
        LocalFileMapPartitionReader partitionReader =
                new LocalFileMapPartitionReader(
                        fileReader,
                        dataListener,
                        StorageTestUtils.NO_OP_BACKLOG_LISTENER,
                        failureListener);
        partitionReader.open();
        return partitionReader;
    }
}
