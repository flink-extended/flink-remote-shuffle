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
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.NoOpDataPartition;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;
import com.alibaba.flink.shuffle.storage.utils.TestDataCommitListener;
import com.alibaba.flink.shuffle.storage.utils.TestDataRegionCreditListener;
import com.alibaba.flink.shuffle.storage.utils.TestFailureListener;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalFileReducePartitionWriter}. */
@RunWith(Parameterized.class)
public class LocalFileReducePartitionWriterTest {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalFileReducePartitionWriterTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testAddAndWriteData() throws Throwable {
        TestReducePartition dataPartition = createBaseReducePartition();
        LocalFileReducePartitionWriter partitionWriter =
                createLocalFileReducePartitionWriter(dataPartition);
        TestReducePartition.TestPartitionWritingTask writingTask =
                dataPartition.getPartitionWritingTask();

        partitionWriter.startRegion(0, false);
        assertEquals(1, writingTask.getNumWritingTriggers());
        assertEquals(1, partitionWriter.numBufferOrMarkers());

        partitionWriter.addBuffer(new ReducePartitionID(0), 0, createBuffer());
        assertEquals(1, writingTask.getNumWritingTriggers());
        assertEquals(2, partitionWriter.numBufferOrMarkers());

        partitionWriter.writeData();
        assertEquals(0, partitionWriter.numBufferOrMarkers());

        partitionWriter.addBuffer(new ReducePartitionID(1), 0, createBuffer());
        assertEquals(2, writingTask.getNumWritingTriggers());
        partitionWriter.writeData();
        assertEquals(0, partitionWriter.numBufferOrMarkers());

        partitionWriter.finishRegion(0);
        TestDataCommitListener commitListener = new TestDataCommitListener();
        partitionWriter.finishDataInput(commitListener);
        assertEquals(3, writingTask.getNumWritingTriggers());
        assertEquals(2, partitionWriter.numBufferOrMarkers());

        partitionWriter.writeData();
        assertEquals(0, partitionWriter.numBufferOrMarkers());
        commitListener.waitForDataCommission();
    }

    @Test
    public void testOnError() throws Throwable {
        TestFailureListener failureListener = new TestFailureListener();
        TestReducePartition dataPartition = createBaseReducePartition();
        LocalFileReducePartitionWriter partitionWriter =
                createLocalFileReducePartitionWriter(dataPartition, failureListener);
        TestReducePartition.TestPartitionWritingTask writingTask =
                dataPartition.getPartitionWritingTask();

        partitionWriter.startRegion(10, false);
        partitionWriter.addBuffer(new ReducePartitionID(0), 0, createBuffer());
        assertEquals(2, partitionWriter.numBufferOrMarkers());
        assertEquals(1, writingTask.getNumWritingTriggers());

        partitionWriter.onError(new ShuffleException("Test."));
        assertEquals(1, partitionWriter.numBufferOrMarkers());
        assertEquals(2, writingTask.getNumWritingTriggers());

        BufferQueue buffers = createBufferQueue();
        buffers.add(ByteBuffer.wrap(StorageTestUtils.DATA_BYTES));
        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(1, buffers.size());

        partitionWriter.release(new ShuffleException("Test."));
        assertFalse(failureListener.isFailed());
    }

    @Test
    public void testAssignCredit() throws Throwable {
        int regionIndex = 0;
        TestDataRegionCreditListener creditListener = new TestDataRegionCreditListener();
        LocalFileReducePartitionWriter partitionWriter =
                createLocalFileReducePartitionWriter(creditListener);

        partitionWriter.startRegion(regionIndex, 1, 100, false);
        partitionWriter.writeData();

        BufferQueue buffers = createBufferQueue();
        for (int i = 1; i < BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY; ++i) {
            buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));
        }

        assertEquals(BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY - 1, buffers.size());
        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(0, buffers.size());
        for (int i = 1; i < BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY; ++i) {
            assertNotNull(creditListener.take(1, regionIndex));
        }
        assertNull(creditListener.take(1, regionIndex));

        partitionWriter.finishRegion(regionIndex);
        partitionWriter.writeData();
        for (int i = 0; i < BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY; ++i) {
            buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));
        }
        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY, buffers.size());
        assertNull(creditListener.take(1, regionIndex));
    }

    @Test
    public void testAssignCreditMoreThanRequired() throws Throwable {
        int regionIndex = 0;
        int numRequired = 5;
        int numAssigned = 8;
        TestDataRegionCreditListener creditListener = new TestDataRegionCreditListener();
        LocalFileReducePartitionWriter partitionWriter =
                createLocalFileReducePartitionWriter(creditListener);

        partitionWriter.startRegion(regionIndex, 1, numRequired, false);
        partitionWriter.writeData();

        BufferQueue buffers = createBufferQueue();
        for (int i = 0; i < numAssigned; ++i) {
            buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));
        }

        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(numAssigned - numRequired, buffers.size());
        for (int i = 0; i < numRequired; ++i) {
            assertNotNull(creditListener.take(1, regionIndex));
        }
        assertNull(creditListener.take(1, regionIndex));
    }

    @Test
    public void testRelease() throws Throwable {
        TestFailureListener failureListener = new TestFailureListener();
        LocalFileReducePartitionWriter partitionWriter =
                createLocalFileReducePartitionWriter(failureListener);

        partitionWriter.startRegion(10, false);
        partitionWriter.addBuffer(new ReducePartitionID(0), 0, createBuffer());
        partitionWriter.writeData();

        partitionWriter.addBuffer(new ReducePartitionID(0), 0, createBuffer());
        partitionWriter.finishRegion(10);
        assertEquals(2, partitionWriter.numBufferOrMarkers());
        assertFalse(failureListener.isFailed());

        partitionWriter.release(new ShuffleException("Test."));
        assertEquals(0, partitionWriter.numBufferOrMarkers());
        assertTrue(failureListener.isFailed());

        BufferQueue buffers = createBufferQueue();
        buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));

        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(1, buffers.size());
    }

    private BufferQueue createBufferQueue() {
        BufferDispatcher dispatcher = new BufferDispatcher("TestDispatcher", 1, 1024);
        DataPartition partition =
                new NoOpDataPartition(
                        new JobID(CommonUtils.randomBytes(16)),
                        new DataSetID(CommonUtils.randomBytes(16)),
                        new MapPartitionID(CommonUtils.randomBytes(16)));
        return new BufferQueue(partition, dispatcher);
    }

    private LocalFileReducePartitionWriter createLocalFileReducePartitionWriter(
            DataRegionCreditListener dataRegionCreditListener) throws IOException {
        return createLocalFileReducePartitionWriter(
                createBaseReducePartition(),
                dataRegionCreditListener,
                StorageTestUtils.NO_OP_FAILURE_LISTENER);
    }

    private LocalFileReducePartitionWriter createLocalFileReducePartitionWriter(
            FailureListener failureListener) throws IOException {
        return createLocalFileReducePartitionWriter(
                createBaseReducePartition(),
                StorageTestUtils.NO_OP_CREDIT_LISTENER,
                failureListener);
    }

    private LocalFileReducePartitionWriter createLocalFileReducePartitionWriter(
            BaseReducePartition dataPartition, FailureListener failureListener) throws IOException {
        return createLocalFileReducePartitionWriter(
                dataPartition, StorageTestUtils.NO_OP_CREDIT_LISTENER, failureListener);
    }

    private LocalFileReducePartitionWriter createLocalFileReducePartitionWriter(
            BaseReducePartition dataPartition) throws IOException {
        LocalFileReducePartitionWriter partitionWriter =
                createLocalFileReducePartitionWriter(
                        dataPartition,
                        StorageTestUtils.NO_OP_CREDIT_LISTENER,
                        StorageTestUtils.NO_OP_FAILURE_LISTENER);
        dataPartition.writers.put(partitionWriter.mapPartitionID, partitionWriter);
        return partitionWriter;
    }

    private LocalFileReducePartitionWriter createLocalFileReducePartitionWriter(
            BaseReducePartition dataPartition,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        LocalReducePartitionFile partitionFile =
                StorageTestUtils.createLocalReducePartitionFile(
                        temporaryFolder.getRoot().getAbsolutePath());
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(
                        partitionFile,
                        dataPartition.getPartitionWritingTask().minBuffersToWrite,
                        dataChecksumEnabled);
        return new LocalFileReducePartitionWriter(
                StorageTestUtils.MAP_PARTITION_ID,
                dataPartition,
                dataRegionCreditListener,
                failureListener,
                fileWriter);
    }

    private Buffer createBuffer() {
        return new Buffer(
                ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE),
                (ignored) -> {},
                StorageTestUtils.DATA_BUFFER_SIZE);
    }

    private TestReducePartition createBaseReducePartition() throws IOException {
        return new TestReducePartition(StorageTestUtils.NO_OP_PARTITIONED_DATA_STORE);
    }
}
