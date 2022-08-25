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

import org.apache.flink.runtime.io.compression.BlockCompressor;

import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static com.alibaba.flink.shuffle.plugin.compression.CompressorUtils.HEADER_LENGTH;
import static com.alibaba.flink.shuffle.plugin.compression.CompressorUtils.writeIntLE;

/** Remote shuffle compressor that wraps {@link Compressor}. */
public class AirBlockCompressor implements BlockCompressor {
    private final Compressor internalCompressor;

    public AirBlockCompressor(Compressor internalCompressor) {
        this.internalCompressor = internalCompressor;
    }

    @Override
    public int getMaxCompressedSize(int srcSize) {
        return HEADER_LENGTH + internalCompressor.maxCompressedLength(srcSize);
    }

    @Override
    public int compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dst, int dstOff)
            throws BufferCompressionException {
        try {
            if (dst.remaining() < dstOff + getMaxCompressedSize(srcLen)) {
                throw new ArrayIndexOutOfBoundsException();
            }

            final int prevSrcOff = src.position() + srcOff;
            final int prevDstOff = dst.position() + dstOff;

            src.position(prevSrcOff);
            dst.position(prevDstOff + HEADER_LENGTH);

            internalCompressor.compress(src, dst);

            int compressedLength = dst.position() - prevDstOff - HEADER_LENGTH;

            dst.position(prevDstOff);
            dst.order(ByteOrder.LITTLE_ENDIAN);
            dst.putInt(compressedLength);
            dst.putInt(srcLen);
            dst.position(prevDstOff + compressedLength + HEADER_LENGTH);

            return HEADER_LENGTH + compressedLength;
        } catch (Exception e) {
            throw new BufferCompressionException(e);
        }
    }

    @Override
    public int compress(byte[] src, int srcOff, int srcLen, byte[] dst, int dstOff)
            throws BufferCompressionException {
        try {
            if (dst.length < dstOff + getMaxCompressedSize(srcLen)) {
                throw new ArrayIndexOutOfBoundsException();
            }

            int compressedLength =
                    internalCompressor.compress(
                            src,
                            srcOff,
                            srcLen,
                            dst,
                            dstOff + HEADER_LENGTH,
                            internalCompressor.maxCompressedLength(srcLen));
            writeIntLE(compressedLength, dst, dstOff);
            writeIntLE(srcLen, dst, dstOff + 4);
            return HEADER_LENGTH + compressedLength;
        } catch (Exception e) {
            throw new BufferCompressionException(e);
        }
    }
}
