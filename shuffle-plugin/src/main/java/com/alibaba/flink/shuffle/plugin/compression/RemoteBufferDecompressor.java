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

package com.alibaba.flink.shuffle.plugin.compression;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.compression.BlockDecompressor;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A new implementation of {@link BufferDecompressor} to decouple with the Flink compressor.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.io.network.buffer.BufferDecompressor).
 */
public class RemoteBufferDecompressor extends BufferDecompressor {

    /** The backing block decompressor for data decompression. */
    private final BlockDecompressor blockDecompressor;

    /** The intermediate buffer for the decompressed data. */
    private final NetworkBuffer internalBuffer;

    /** The backup array of intermediate buffer. */
    private final byte[] internalBufferArray;

    public RemoteBufferDecompressor(int bufferSize, String factoryName) {
        super(bufferSize, NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC.defaultValue());
        checkArgument(bufferSize > 0);
        checkNotNull(factoryName);

        // the decompressed data size should be never larger than the configured buffer size
        this.internalBufferArray = new byte[bufferSize];
        this.internalBuffer =
                new NetworkBuffer(
                        MemorySegmentFactory.wrap(internalBufferArray),
                        FreeingBufferRecycler.INSTANCE);
        this.blockDecompressor =
                RemoteBlockCompressionFactory.createBlockCompressionFactory(factoryName)
                        .getDecompressor();
    }

    /**
     * Decompresses the given {@link Buffer} using {@link BlockDecompressor}. The decompressed data
     * will be stored in the intermediate buffer of this {@link
     * org.apache.flink.runtime.io.network.buffer.BufferDecompressor} and returned to the caller.
     * The caller must guarantee that the returned {@link Buffer} has been freed when calling the
     * method next time.
     *
     * <p>Notes that the decompression will always start from offset 0 to the size of the input
     * {@link Buffer}.
     */
    @Override
    public Buffer decompressToIntermediateBuffer(Buffer buffer) {
        int decompressedLen = decompress(buffer);
        internalBuffer.setSize(decompressedLen);

        return internalBuffer.retainBuffer();
    }

    /**
     * The difference between this method and {@link #decompressToIntermediateBuffer(Buffer)} is
     * that this method copies the decompressed data to the input {@link Buffer} starting from
     * offset 0.
     *
     * <p>The caller must guarantee that the input {@link Buffer} is writable and there's enough
     * space left.
     */
    @Override
    @VisibleForTesting
    public Buffer decompressToOriginalBuffer(Buffer buffer) {
        int decompressedLen = decompress(buffer);

        // copy the decompressed data back
        int memorySegmentOffset = buffer.getMemorySegmentOffset();
        MemorySegment segment = buffer.getMemorySegment();
        segment.put(memorySegmentOffset, internalBufferArray, 0, decompressedLen);

        return new ReadOnlySlicedNetworkBuffer(
                buffer.asByteBuf(), 0, decompressedLen, memorySegmentOffset, false);
    }

    /**
     * Decompresses the input {@link Buffer} into the intermediate buffer and returns the
     * decompressed data size.
     */
    private int decompress(Buffer buffer) {
        checkArgument(buffer != null, "The input buffer must not be null.");
        checkArgument(buffer.isBuffer(), "Event can not be decompressed.");
        checkArgument(buffer.isCompressed(), "Buffer not compressed.");
        checkArgument(buffer.getReaderIndex() == 0, "Reader index of the input buffer must be 0.");
        checkArgument(buffer.readableBytes() > 0, "No data to be decompressed.");
        checkState(
                internalBuffer.refCnt() == 1,
                "Illegal reference count, buffer need to be released.");

        int length = buffer.getSize();
        MemorySegment memorySegment = buffer.getMemorySegment();
        // If buffer is on-heap, manipulate the underlying array directly. There are two main
        // reasons why NIO buffer is not directly used here: One is that some compression
        // libraries will use the underlying array for heap buffer, but our input buffer may be
        // a read-only ByteBuffer, and it is illegal to access internal array. Another reason
        // is that for the on-heap buffer, directly operating the underlying array can reduce
        // additional overhead compared to generating a NIO buffer.
        if (!memorySegment.isOffHeap()) {
            return blockDecompressor.decompress(
                    memorySegment.getArray(),
                    buffer.getMemorySegmentOffset(),
                    length,
                    internalBufferArray,
                    0);
        } else {
            // decompress the given buffer into the internal heap buffer
            return blockDecompressor.decompress(
                    buffer.getNioBuffer(0, length),
                    0,
                    length,
                    internalBuffer.getNioBuffer(0, internalBuffer.capacity()),
                    0);
        }
    }
}
