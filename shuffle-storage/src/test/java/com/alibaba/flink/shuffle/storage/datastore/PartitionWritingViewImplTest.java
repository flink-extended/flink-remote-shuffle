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

package com.alibaba.flink.shuffle.storage.datastore;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link PartitionWritingViewImpl}. */
public class PartitionWritingViewImplTest {

    private final DataPartitionWriter partitionWriter = new NoOpDataPartitionWriter();

    private final DataCommitListener noOpDataCommitListener = () -> {};

    @Test(expected = IllegalStateException.class)
    public void testOnErrorAfterFinish() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.finish(noOpDataCommitListener);
        writingView.onError(new ShuffleException("Test exception."));
    }

    @Test(expected = IllegalStateException.class)
    public void testOnErrorAfterError() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.onError(new ShuffleException("Test exception."));
        writingView.onError(new ShuffleException("Test exception."));
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishAfterFinish() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.finish(noOpDataCommitListener);
        writingView.finish(noOpDataCommitListener);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishAfterError() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.onError(new ShuffleException("Test exception."));
        writingView.finish(noOpDataCommitListener);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullDataCommitListener() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.finish(null);
    }

    @Test(expected = IllegalStateException.class)
    public void testRegionFinishAfterFinish() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.finish(noOpDataCommitListener);
        writingView.regionFinished();
    }

    @Test(expected = IllegalStateException.class)
    public void testRegionFinishAfterError() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.onError(new ShuffleException("Test exception."));
        writingView.regionFinished();
    }

    @Test(expected = IllegalStateException.class)
    public void testRegionStartAfterFinish() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.finish(noOpDataCommitListener);
        writingView.regionStarted(0, false);
    }

    @Test(expected = IllegalStateException.class)
    public void testRegionStartAfterError() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.onError(new ShuffleException("Test exception."));
        writingView.regionStarted(0, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalDataRegionIndex() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.regionStarted(-1, false);
    }

    @Test
    public void testOnBufferAfterError() {
        AtomicReference<ByteBuffer> recycleBuffer = new AtomicReference<>();
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.regionStarted(10, true);

        writingView.onError(new ShuffleException("Test exception."));
        Buffer buffer = new Buffer(ByteBuffer.allocateDirect(1024), recycleBuffer::set, 1024);
        assertThrows(
                IllegalStateException.class,
                () -> writingView.onBuffer(buffer, new ReducePartitionID(0)));

        assertNotNull(recycleBuffer.get());
    }

    @Test
    public void testOnBufferAfterFinish() {
        AtomicReference<ByteBuffer> recycleBuffer = new AtomicReference<>();
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        writingView.finish(noOpDataCommitListener);
        Buffer buffer = new Buffer(ByteBuffer.allocateDirect(1024), recycleBuffer::set, 1024);
        assertThrows(
                IllegalStateException.class,
                () -> writingView.onBuffer(buffer, new ReducePartitionID(0)));

        assertNotNull(recycleBuffer.get());
    }

    @Test
    public void testOnBufferWithNullReducePartitionID() {
        AtomicReference<ByteBuffer> recycleBuffer = new AtomicReference<>();
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.regionStarted(10, true);

        Buffer buffer = new Buffer(ByteBuffer.allocateDirect(1024), recycleBuffer::set, 1024);
        assertThrows(IllegalArgumentException.class, () -> writingView.onBuffer(buffer, null));

        assertNotNull(recycleBuffer.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOnNullBuffer() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.onBuffer(null, new ReducePartitionID(0));
    }

    @Test
    public void testOnBufferBeforeRegionStart() {
        AtomicReference<ByteBuffer> recycleBuffer = new AtomicReference<>();
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);

        Buffer buffer = new Buffer(ByteBuffer.allocateDirect(1024), recycleBuffer::set, 1024);
        assertThrows(IllegalStateException.class, () -> writingView.onBuffer(buffer, null));

        assertNotNull(recycleBuffer.get());
    }

    @Test(expected = IllegalStateException.class)
    public void testRegionFinishBeforeStart() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.regionFinished();
    }

    @Test(expected = IllegalStateException.class)
    public void testRegionStartBeforeFinish() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.regionStarted(10, true);
        writingView.regionStarted(10, true);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishBeforeRegionFinish() {
        DataPartitionWritingView writingView = new PartitionWritingViewImpl(partitionWriter);
        writingView.regionStarted(10, true);
        writingView.finish(noOpDataCommitListener);
    }
}
