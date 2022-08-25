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
import org.apache.flink.runtime.io.compression.BlockDecompressor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;

import static com.alibaba.flink.shuffle.plugin.compression.CompressorUtils.HEADER_LENGTH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for block compression. */
@RunWith(Parameterized.class)
public class BlockCompressionTest {
    private final RemoteBlockCompressionFactory factory;

    @Parameterized.Parameters
    public static Object[] data() {
        return new RemoteBlockCompressionFactory[] {
            RemoteBlockCompressionFactory.createBlockCompressionFactory("LZ4"),
            RemoteBlockCompressionFactory.createBlockCompressionFactory("LZO"),
            RemoteBlockCompressionFactory.createBlockCompressionFactory("ZSTD")
        };
    }

    public BlockCompressionTest(RemoteBlockCompressionFactory factory) {
        this.factory = factory;
    }

    @Test
    public void testBlockCompression() {
        runArrayTest(factory, 32768);
        runArrayTest(factory, 16);

        runByteBufferTest(factory, false, 32768);
        runByteBufferTest(factory, false, 16);
        runByteBufferTest(factory, true, 32768);
        runByteBufferTest(factory, true, 16);
    }

    private void runArrayTest(RemoteBlockCompressionFactory factory, int originalLen) {
        BlockCompressor compressor = factory.getCompressor();
        BlockDecompressor decompressor = factory.getDecompressor();

        int originalOff = 64;
        byte[] data = new byte[originalOff + originalLen];
        for (int i = 0; i < originalLen; i++) {
            data[originalOff + i] = (byte) i;
        }

        int compressedOff = 32;

        // 1. test compress with insufficient target
        byte[] insufficientCompressArray = new byte[compressedOff + HEADER_LENGTH + 1];
        assertThrows(
                RuntimeException.class,
                () ->
                        compressor.compress(
                                data,
                                originalOff,
                                originalLen,
                                insufficientCompressArray,
                                compressedOff));

        // 2. test normal compress
        byte[] compressedData =
                new byte[compressedOff + compressor.getMaxCompressedSize(originalLen)];
        int compressedLen =
                compressor.compress(data, originalOff, originalLen, compressedData, compressedOff);

        int decompressedOff = 16;

        // 3. test decompress with insufficient target
        byte[] insufficientDecompressArray = new byte[decompressedOff + originalLen - 1];
        assertThrows(
                RuntimeException.class,
                () ->
                        decompressor.decompress(
                                compressedData,
                                compressedOff,
                                compressedLen,
                                insufficientDecompressArray,
                                decompressedOff));

        // 4. test normal decompress
        byte[] decompressedData = new byte[decompressedOff + originalLen];
        int decompressedLen =
                decompressor.decompress(
                        compressedData,
                        compressedOff,
                        compressedLen,
                        decompressedData,
                        decompressedOff);
        assertEquals(decompressedLen, originalLen);

        for (int i = 0; i < originalLen; i++) {
            assertEquals(decompressedData[decompressedOff + i], data[originalOff + i]);
        }
    }

    private void runByteBufferTest(
            RemoteBlockCompressionFactory factory, boolean isDirect, int originalLen) {
        BlockCompressor compressor = factory.getCompressor();
        BlockDecompressor decompressor = factory.getDecompressor();

        int originalOff = 64;
        ByteBuffer data;
        if (isDirect) {
            data = ByteBuffer.allocateDirect(originalOff + originalLen);
        } else {
            data = ByteBuffer.allocate(originalOff + originalLen);
        }

        // Useless data
        for (int i = 0; i < originalOff; i++) {
            data.put((byte) 0x5a);
        }

        for (int i = 0; i < originalLen; i++) {
            data.put((byte) i);
        }
        data.flip();

        ByteBuffer compressedData;
        int maxCompressedLen = compressor.getMaxCompressedSize(originalLen);
        if (isDirect) {
            compressedData = ByteBuffer.allocateDirect(maxCompressedLen);
        } else {
            compressedData = ByteBuffer.allocate(maxCompressedLen);
        }
        int compressedLen = compressor.compress(data, originalOff, originalLen, compressedData, 0);
        assertEquals(compressedData.position(), compressedLen);
        compressedData.flip();

        int compressedOff = 32;
        ByteBuffer copiedCompressedData;
        if (isDirect) {
            copiedCompressedData = ByteBuffer.allocateDirect(compressedOff + compressedLen);
        } else {
            copiedCompressedData = ByteBuffer.allocate(compressedOff + compressedLen);
        }

        // Useless data
        for (int i = 0; i < compressedOff; i++) {
            copiedCompressedData.put((byte) 0x5a);
        }

        byte[] compressedByteArray = new byte[compressedLen];
        compressedData.get(compressedByteArray, 0, compressedLen);
        copiedCompressedData.put(compressedByteArray);
        copiedCompressedData.flip();

        ByteBuffer decompressedData;
        if (isDirect) {
            decompressedData = ByteBuffer.allocateDirect(originalLen);
        } else {
            decompressedData = ByteBuffer.allocate(originalLen);
        }
        int decompressedLen =
                decompressor.decompress(
                        copiedCompressedData, compressedOff, compressedLen, decompressedData, 0);
        assertEquals(decompressedData.position(), decompressedLen);
        decompressedData.flip();

        for (int i = 0; i < decompressedLen; i++) {
            assertEquals(decompressedData.get(), (byte) i);
        }
    }
}
