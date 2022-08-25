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

import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * A new implementation of {@link BufferCompressor} to decouple with the Flink compressor.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.io.network.buffer.BufferCompressor).
 */
public class RemoteBufferCompressor extends BufferCompressor {

    private final BlockCompressor blockCompressor;
    private final NetworkBuffer internalBuffer;

    public RemoteBufferCompressor(int bufferSize, String factoryName) {
        super(bufferSize, NettyShuffleEnvironmentOptions.SHUFFLE_COMPRESSION_CODEC.defaultValue());
        byte[] heapBuffer = new byte[2 * bufferSize];
        this.internalBuffer =
                new NetworkBuffer(
                        MemorySegmentFactory.wrap(heapBuffer), FreeingBufferRecycler.INSTANCE);
        this.blockCompressor =
                RemoteBlockCompressionFactory.createBlockCompressionFactory(factoryName)
                        .getCompressor();
    }

    @Override
    public Buffer compressToIntermediateBuffer(Buffer buffer) {
        int compressedLen;
        if ((compressedLen = this.compress(buffer)) == 0) {
            return buffer;
        } else {
            this.internalBuffer.setCompressed(true);
            this.internalBuffer.setSize(compressedLen);
            return this.internalBuffer.retainBuffer();
        }
    }

    @Override
    public Buffer compressToOriginalBuffer(Buffer buffer) {
        int compressedLen;
        if ((compressedLen = this.compress(buffer)) == 0) {
            return buffer;
        } else {
            int memorySegmentOffset = buffer.getMemorySegmentOffset();
            MemorySegment segment = buffer.getMemorySegment();
            segment.put(memorySegmentOffset, this.internalBuffer.array(), 0, compressedLen);
            return new ReadOnlySlicedNetworkBuffer(
                    buffer.asByteBuf(), 0, compressedLen, memorySegmentOffset, true);
        }
    }

    private int compress(Buffer buffer) {
        checkArgument(buffer != null, "The input buffer must not be null.");
        checkArgument(buffer.isBuffer(), "Event can not be compressed.");
        checkArgument(!buffer.isCompressed(), "Buffer already compressed.");
        checkArgument(buffer.getReaderIndex() == 0, "Reader index of the input buffer must be 0.");
        checkArgument(buffer.readableBytes() > 0, "No data to be compressed.");
        checkState(
                this.internalBuffer.refCnt() == 1,
                "Illegal reference count, buffer need to be released.");

        try {
            int length = buffer.getSize();
            int compressedLen =
                    this.blockCompressor.compress(
                            buffer.getNioBuffer(0, length),
                            0,
                            length,
                            this.internalBuffer.getNioBuffer(0, this.internalBuffer.capacity()),
                            0);
            return compressedLen < length ? compressedLen : 0;
        } catch (Throwable var4) {
            return 0;
        }
    }
}
