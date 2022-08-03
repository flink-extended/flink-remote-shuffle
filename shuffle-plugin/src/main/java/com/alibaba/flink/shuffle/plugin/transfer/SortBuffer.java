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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * Data of different channels can be appended to a {@link SortBuffer}., after apending finished,
 * data can be copied from it in channel index order.
 */
public interface SortBuffer {

    /**
     * Appends data of the specified channel to this {@link SortBuffer} and returns true if all
     * bytes of the source buffer is copied to this {@link SortBuffer} successfully, otherwise if
     * returns false, nothing will be copied.
     */
    boolean append(ByteBuffer source, int targetChannel, Buffer.DataType dataType)
            throws IOException;

    /**
     * Copies data from this {@link SortBuffer} to the target {@link MemorySegment} in channel index
     * order and returns {@link BufferWithChannel} which contains the copied data and the
     * corresponding channel index.
     */
    BufferWithChannel copyIntoSegment(MemorySegment target, BufferRecycler recycler, int offset);

    /**
     * Copies data from this {@link SortBuffer} to the target {@link MemorySegment} of specific
     * channel index and returns {@link BufferWithChannel} which contains the copied data and the
     * corresponding channel index.
     */
    BufferWithChannel copyChannelBuffersIntoSegment(
            MemorySegment target, int targetChannelIndex, BufferRecycler recycler, int offset);

    /** Returns the sub partition read index of this {@link SortBuffer}. */
    int getSubpartitionReadOrderIndex(int channelIndex);

    /** Returns the number of records written to this {@link SortBuffer}. */
    long numRecords();

    /** Returns the number of bytes written to this {@link SortBuffer}. */
    long numBytes();

    /** Returns the number of bytes for the specific target subpartition. */
    long numSubpartitionBytes(int targetSubpartition);

    /** Returns the number of events for the specific target subpartition. */
    int numEvents(int targetSubpartition);

    /** Returns true if there is no data can be consumed for the specific target subpartition. */
    boolean hasSubpartitionReadFinish(int targetSubpartition);

    /** Returns true if there is still data can be consumed in this {@link SortBuffer}. */
    boolean hasRemaining();

    /** Finishes this {@link SortBuffer} which means no record can be appended any more. */
    void finish();

    /** Whether this {@link SortBuffer} is finished or not. */
    boolean isFinished();

    /** Releases this {@link SortBuffer} which releases all resources. */
    void release();

    /** Whether this {@link SortBuffer} is released or not. */
    boolean isReleased();

    /** Buffer and the corresponding channel index returned to reader. */
    class BufferWithChannel {

        private final Buffer buffer;

        private final int channelIndex;

        BufferWithChannel(Buffer buffer, int channelIndex) {
            this.buffer = checkNotNull(buffer);
            this.channelIndex = channelIndex;
        }

        /** Get {@link Buffer}. */
        public Buffer getBuffer() {
            return buffer;
        }

        /** Get channel index. */
        public int getChannelIndex() {
            return channelIndex;
        }
    }
}
