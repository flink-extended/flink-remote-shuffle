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

/** Tests for {@link LocalFileMapPartitionWriter}. */
@RunWith(Parameterized.class)
public class LocalFileMapPartitionWriterTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalFileMapPartitionWriterTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testAddAndWriteData() throws Throwable {
        TestMapPartition dataPartition = createBaseMapPartition();
        LocalFileMapPartitionWriter partitionWriter =
                createLocalFileMapPartitionWriter(dataPartition);
        TestMapPartition.TestPartitionWritingTask writingTask =
                dataPartition.getPartitionWritingTask();

        partitionWriter.startRegion(0, false);
        assertEquals(1, writingTask.getNumWritingTriggers());
        assertEquals(1, partitionWriter.getNumPendingBuffers());

        partitionWriter.addBuffer(new ReducePartitionID(0), createBuffer());
        assertEquals(1, writingTask.getNumWritingTriggers());
        assertEquals(2, partitionWriter.getNumPendingBuffers());

        partitionWriter.writeData();
        assertEquals(0, partitionWriter.getNumPendingBuffers());

        partitionWriter.addBuffer(new ReducePartitionID(1), createBuffer());
        assertEquals(2, writingTask.getNumWritingTriggers());
        assertEquals(1, partitionWriter.getNumPendingBuffers());

        partitionWriter.finishRegion();
        TestDataCommitListener commitListener = new TestDataCommitListener();
        partitionWriter.finishDataInput(commitListener);
        assertEquals(2, writingTask.getNumWritingTriggers());
        assertEquals(3, partitionWriter.getNumPendingBuffers());

        partitionWriter.writeData();
        assertEquals(0, partitionWriter.getNumPendingBuffers());
        commitListener.waitForDataCommission();
    }

    @Test
    public void testOnError() throws Throwable {
        TestFailureListener failureListener = new TestFailureListener();
        TestMapPartition dataPartition = createBaseMapPartition();
        LocalFileMapPartitionWriter partitionWriter =
                createLocalFileMapPartitionWriter(dataPartition, failureListener);
        TestMapPartition.TestPartitionWritingTask writingTask =
                dataPartition.getPartitionWritingTask();

        partitionWriter.startRegion(10, false);
        partitionWriter.addBuffer(new ReducePartitionID(0), createBuffer());
        assertEquals(2, partitionWriter.getNumPendingBuffers());
        assertEquals(1, writingTask.getNumWritingTriggers());

        partitionWriter.onError(new ShuffleException("Test."));
        assertEquals(1, partitionWriter.getNumPendingBuffers());
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
        LocalFileMapPartitionWriter partitionWriter =
                createLocalFileMapPartitionWriter(creditListener);

        partitionWriter.startRegion(regionIndex, false);
        partitionWriter.writeData();

        BufferQueue buffers = createBufferQueue();
        for (int i = 1; i < BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY; ++i) {
            buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));
        }

        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY - 1, buffers.size());
        assertNull(creditListener.take(1, regionIndex));

        buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));

        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(0, buffers.size());
        for (int i = 0; i < BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY; ++i) {
            assertNotNull(creditListener.take(0, regionIndex));
        }

        partitionWriter.finishRegion();
        partitionWriter.writeData();
        for (int i = 0; i < BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY; ++i) {
            buffers.add(ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE));
        }
        partitionWriter.assignCredits(buffers, (ignored) -> {});
        assertEquals(BaseDataPartitionWriter.MIN_CREDITS_TO_NOTIFY, buffers.size());
        assertNull(creditListener.take(1, regionIndex));
    }

    @Test
    public void testRelease() throws Throwable {
        TestFailureListener failureListener = new TestFailureListener();
        LocalFileMapPartitionWriter partitionWriter =
                createLocalFileMapPartitionWriter(failureListener);

        partitionWriter.startRegion(10, false);
        partitionWriter.addBuffer(new ReducePartitionID(0), createBuffer());
        partitionWriter.writeData();

        partitionWriter.addBuffer(new ReducePartitionID(0), createBuffer());
        partitionWriter.finishRegion();
        assertEquals(2, partitionWriter.getNumPendingBuffers());
        assertFalse(failureListener.isFailed());

        partitionWriter.release(new ShuffleException("Test."));
        assertEquals(0, partitionWriter.getNumPendingBuffers());
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

    private LocalFileMapPartitionWriter createLocalFileMapPartitionWriter(
            DataRegionCreditListener dataRegionCreditListener) throws IOException {
        return createLocalFileMapPartitionWriter(
                createBaseMapPartition(),
                dataRegionCreditListener,
                StorageTestUtils.NO_OP_FAILURE_LISTENER);
    }

    private LocalFileMapPartitionWriter createLocalFileMapPartitionWriter(
            FailureListener failureListener) throws IOException {
        return createLocalFileMapPartitionWriter(
                createBaseMapPartition(), StorageTestUtils.NO_OP_CREDIT_LISTENER, failureListener);
    }

    private LocalFileMapPartitionWriter createLocalFileMapPartitionWriter(
            BaseMapPartition dataPartition, FailureListener failureListener) throws IOException {
        return createLocalFileMapPartitionWriter(
                dataPartition, StorageTestUtils.NO_OP_CREDIT_LISTENER, failureListener);
    }

    private LocalFileMapPartitionWriter createLocalFileMapPartitionWriter(
            BaseMapPartition dataPartition) throws IOException {
        return createLocalFileMapPartitionWriter(
                dataPartition,
                StorageTestUtils.NO_OP_CREDIT_LISTENER,
                StorageTestUtils.NO_OP_FAILURE_LISTENER);
    }

    private LocalFileMapPartitionWriter createLocalFileMapPartitionWriter(
            BaseMapPartition dataPartition,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener)
            throws IOException {
        LocalMapPartitionFile partitionFile =
                StorageTestUtils.createLocalMapPartitionFile(
                        temporaryFolder.getRoot().getAbsolutePath());
        return new LocalFileMapPartitionWriter(
                dataChecksumEnabled,
                StorageTestUtils.MAP_PARTITION_ID,
                dataPartition,
                dataRegionCreditListener,
                failureListener,
                partitionFile);
    }

    private Buffer createBuffer() {
        return new Buffer(
                ByteBuffer.allocateDirect(StorageTestUtils.DATA_BUFFER_SIZE),
                (ignored) -> {},
                StorageTestUtils.DATA_BUFFER_SIZE);
    }

    private TestMapPartition createBaseMapPartition() {
        return new TestMapPartition(StorageTestUtils.NO_OP_PARTITIONED_DATA_STORE);
    }
}
