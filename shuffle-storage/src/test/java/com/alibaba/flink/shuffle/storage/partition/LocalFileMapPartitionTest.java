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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;
import com.alibaba.flink.shuffle.storage.exception.ConcurrentWriteException;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;
import com.alibaba.flink.shuffle.storage.utils.TestDataCommitListener;
import com.alibaba.flink.shuffle.storage.utils.TestDataRegionCreditListener;
import com.alibaba.flink.shuffle.storage.utils.TestFailureListener;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalFileMapPartition}. */
public class LocalFileMapPartitionTest {

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWriteAndReadPartition() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        int buffersWritten = writeLocalFileMapPartition(dataPartition, 10, false, true);
        int buffersRead = readLocalFileMapPartition(dataPartition, false);

        assertEquals(buffersWritten, buffersRead);
    }

    @Test
    public void testWriteAndReadEmptyPartition() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        int buffersWritten = writeLocalFileMapPartition(dataPartition, 0, false, true);
        int buffersRead = readLocalFileMapPartition(dataPartition, false);

        assertEquals(0, buffersRead);
        assertEquals(0, buffersWritten);
    }

    @Test
    public void testReleaseWhileWriting() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        LocalFileMapPartition dataPartition = createLocalFileMapPartition();
        Thread writingThread =
                new Thread(
                        () -> {
                            CommonUtils.runQuietly(
                                    () ->
                                            writeLocalFileMapPartition(
                                                    dataPartition, 10, false, true));
                            latch.countDown();
                        });
        writingThread.start();

        Thread.sleep(10);
        dataPartition.releasePartition(new ShuffleException("Test exception.")).get();
        latch.await();

        StorageTestUtils.assertNoBufferLeaking();
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles()).length);
    }

    @Test
    public void testReleaseWhileReading() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        writeLocalFileMapPartition(dataPartition, 10, false, true);
        Thread readingThread =
                new Thread(
                        () -> {
                            CommonUtils.runQuietly(
                                    () -> readLocalFileMapPartition(dataPartition, false));
                            latch.countDown();
                        });
        readingThread.start();

        Thread.sleep(10);
        dataPartition.releasePartition(new ShuffleException("Test exception.")).get();
        latch.await();

        StorageTestUtils.assertNoBufferLeaking();
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles()).length);
    }

    @Test
    public void testOnErrorWhileWriting() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();
        writeLocalFileMapPartition(dataPartition, 10, true, true);

        StorageTestUtils.assertNoBufferLeaking();
    }

    @Test
    public void testOnErrorWhileReading() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        writeLocalFileMapPartition(dataPartition, 10, false, true);
        readLocalFileMapPartition(dataPartition, true);

        StorageTestUtils.assertNoBufferLeaking();
    }

    @Test
    public void testIsConsumableOfReleasedPartition() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        writeLocalFileMapPartition(dataPartition, 10, false, true);
        dataPartition.releasePartition(new ShuffleException("Test exception.")).get();
        assertFalse(dataPartition.isConsumable());
        StorageTestUtils.assertNoBufferLeaking();
    }

    @Test
    public void testIsConsumableOfUnfinishedPartition() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        DataPartitionWriter partitionWriter =
                dataPartition.createPartitionWriter(
                        dataPartition.getPartitionMeta().getDataPartitionID(),
                        StorageTestUtils.NO_OP_CREDIT_LISTENER,
                        StorageTestUtils.NO_OP_FAILURE_LISTENER);
        partitionWriter.startRegion(10, false);
        assertFalse(dataPartition.isConsumable());

        dataPartition.releasePartition(new ShuffleException("Test exception.")).get();
        StorageTestUtils.assertNoBufferLeaking();
    }

    @Test
    public void testWritePartitionFileError() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        LocalFileMapPartition dataPartition = createLocalFileMapPartition();
        Thread writingThread =
                new Thread(
                        () -> {
                            CommonUtils.runQuietly(
                                    () ->
                                            writeLocalFileMapPartition(
                                                    dataPartition, 10, false, false));
                            latch.countDown();
                        });
        writingThread.start();

        Thread.sleep(100);
        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            Files.deleteIfExists(file.toPath());
        }
        latch.await();

        StorageTestUtils.assertNoBufferLeaking();
    }

    @Test
    public void testReadPartitionFileError() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        writeLocalFileMapPartition(dataPartition, 10, false, true);
        Thread readingThread =
                new Thread(
                        () -> {
                            CommonUtils.runQuietly(
                                    () -> readLocalFileMapPartition(dataPartition, false), true);
                            latch.countDown();
                        });
        readingThread.start();

        Thread.sleep(10);
        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            Files.delete(file.toPath());
        }
        latch.await();

        StorageTestUtils.assertNoBufferLeaking();
    }

    @Test
    public void testWritePartitionMultipleTimes() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();

        writeLocalFileMapPartition(dataPartition, 0, false, true);
        TestFailureListener failureListener = new TestFailureListener();
        CommonUtils.runQuietly(
                () -> writeLocalFileMapPartition(dataPartition, 0, false, false, failureListener));
        assertTrue(failureListener.getFailure().getCause() instanceof ConcurrentWriteException);
    }

    @Test
    public void testDeletePartitionIndexFile() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();
        writeLocalFileMapPartition(dataPartition, 10, false, true);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.INDEX_FILE_SUFFIX)) {
                Files.delete(file.toPath());
            }
        }
        assertFalse(dataPartition.isConsumable());
    }

    @Test
    public void testDeletePartitionDataFile() throws Exception {
        LocalFileMapPartition dataPartition = createLocalFileMapPartition();
        writeLocalFileMapPartition(dataPartition, 10, false, true);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.DATA_FILE_SUFFIX)) {
                Files.delete(file.toPath());
            }
        }
        assertFalse(dataPartition.isConsumable());
    }

    private LocalFileMapPartition createLocalFileMapPartition() {
        String storagePath = temporaryFolder.getRoot().getAbsolutePath() + "/";
        return new LocalFileMapPartition(
                new StorageMeta(storagePath, StorageType.SSD, storagePath),
                StorageTestUtils.NO_OP_PARTITIONED_DATA_STORE,
                StorageTestUtils.JOB_ID,
                StorageTestUtils.DATA_SET_ID,
                StorageTestUtils.MAP_PARTITION_ID,
                StorageTestUtils.NUM_REDUCE_PARTITIONS);
    }

    private int writeLocalFileMapPartition(
            LocalFileMapPartition dataPartition,
            int numRegions,
            boolean isError,
            boolean waitDataCommission)
            throws Exception {
        return writeLocalFileMapPartition(
                dataPartition,
                numRegions,
                isError,
                waitDataCommission,
                StorageTestUtils.NO_OP_FAILURE_LISTENER);
    }

    private int writeLocalFileMapPartition(
            LocalFileMapPartition dataPartition,
            int numRegions,
            boolean isError,
            boolean waitDataCommission,
            FailureListener failureListener)
            throws Exception {
        TestDataRegionCreditListener creditListener = new TestDataRegionCreditListener();
        DataPartitionWriter partitionWriter =
                dataPartition.createPartitionWriter(
                        dataPartition.getPartitionMeta().getDataPartitionID(),
                        creditListener,
                        failureListener);

        int buffersWritten = 0;
        int numBuffers = 100;

        for (int regionIndex = 0; regionIndex < numRegions; ++regionIndex) {
            partitionWriter.startRegion(regionIndex, false);
            for (int reduceIndex = 0;
                    reduceIndex < StorageTestUtils.NUM_REDUCE_PARTITIONS;
                    ++reduceIndex) {
                for (int bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex) {
                    Buffer buffer;
                    while ((buffer = partitionWriter.pollBuffer()) == null) {
                        creditListener.take(100, regionIndex);
                    }

                    buffer.writeBytes(StorageTestUtils.DATA_BYTES);
                    partitionWriter.addBuffer(new ReducePartitionID(reduceIndex), buffer);
                    ++buffersWritten;
                }

                if (isError) {
                    partitionWriter.onError(new ShuffleException("Test exception."));
                    return buffersWritten;
                }
            }
            partitionWriter.finishRegion();
        }

        TestDataCommitListener commitListener = new TestDataCommitListener();
        partitionWriter.finishDataInput(commitListener);
        if (waitDataCommission) {
            commitListener.waitForDataCommission();
        }
        return buffersWritten;
    }

    public int readLocalFileMapPartition(LocalFileMapPartition dataPartition, boolean isError)
            throws Exception {
        ConcurrentHashMap<DataPartitionReader, TestFailureListener> readers =
                new ConcurrentHashMap<>();
        for (int reduceIndex = 0;
                reduceIndex < StorageTestUtils.NUM_REDUCE_PARTITIONS;
                ++reduceIndex) {
            TestFailureListener failureListener = new TestFailureListener();
            final int finalReduceIndex = reduceIndex;
            CommonUtils.runQuietly(
                    () -> {
                        DataPartitionReader reader =
                                dataPartition.createPartitionReader(
                                        finalReduceIndex,
                                        finalReduceIndex,
                                        StorageTestUtils.NO_OP_DATA_LISTENER,
                                        StorageTestUtils.NO_OP_BACKLOG_LISTENER,
                                        failureListener);
                        readers.put(reader, failureListener);
                    });
        }

        int buffersRead = 0;
        while (!readers.isEmpty()) {
            for (DataPartitionReader reader : readers.keySet()) {
                BufferWithBacklog buffer;
                while ((buffer = reader.nextBuffer()) != null) {
                    assertEquals(
                            ByteBuffer.wrap(StorageTestUtils.DATA_BYTES),
                            buffer.getBuffer().nioBuffer());
                    BufferUtils.recycleBuffer(buffer.getBuffer());
                    ++buffersRead;
                }

                if (reader.isFinished() || readers.get(reader).isFailed()) {
                    readers.remove(reader);
                }

                if (isError) {
                    reader.onError(new ShuffleException("Test exception."));
                    readers.remove(reader);
                }
            }
        }
        return buffersRead;
    }
}
