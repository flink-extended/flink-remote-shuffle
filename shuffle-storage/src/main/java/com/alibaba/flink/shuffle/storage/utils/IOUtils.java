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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.storage.exception.FileCorruptedException;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/** Utility methods for IO. */
public class IOUtils {

    private static final Logger LOG = LoggerFactory.getLogger(IOUtils.class);

    /**
     * Size of buffer header: 4 bytes for buffer length, 4 bytes for magic number and 8 bytes for
     * checksum.
     */
    public static final int HEADER_BUFFER_SIZE = 4 + 4 + 8;

    /**
     * Magic number used to check whether the data has corrupted or not. Note that the data is not
     * guaranteed to be in good state even when the magic number is correct.
     */
    public static final int MAGIC_NUMBER = 1431655765; // 01010101010101010101010101010101

    /**
     * Magic number used to check whether the data has corrupted or not. Note that the data is not
     * guaranteed to be in good state even when the magic number is correct.
     */
    public static final int MAGIC_NUMBER_WITHOUT_CHECKSUM =
            MAGIC_NUMBER; // 01010101010101010101010101010101

    /**
     * Magic number used to check whether the data has corrupted or not. Note that the data is not
     * guaranteed to be in good state even when the magic number is correct.
     */
    public static final int MAGIC_NUMBER_WITH_CHECKSUM =
            ~MAGIC_NUMBER; // 10101010101010101010101010101010

    /** Opens a {@link FileChannel} for writing, will fail if the file already exists. */
    public static FileChannel createWritableFileChannel(Path path) throws IOException {
        CommonUtils.checkArgument(path != null, "Must be not null.");

        return FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    /** Opens a {@link FileChannel} for reading. */
    public static FileChannel openReadableFileChannel(Path path) throws IOException {
        CommonUtils.checkArgument(path != null, "Must be not null.");

        return FileChannel.open(path, StandardOpenOption.READ);
    }

    /** Writes all data of the given {@link ByteBuffer} to the target {@link FileChannel}. */
    public static void writeBuffer(FileChannel fileChannel, ByteBuffer buffer) throws IOException {
        CommonUtils.checkArgument(fileChannel != null, "Must be not null.");
        CommonUtils.checkArgument(buffer != null, "Must be not null.");

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

    /** Writes a collection of {@link ByteBuffer}s to the target {@link FileChannel}. */
    public static long writeBuffers(FileChannel fileChannel, ByteBuffer[] buffers)
            throws IOException {
        CommonUtils.checkArgument(fileChannel != null, "Must be not null.");
        CommonUtils.checkArgument(buffers != null, "Must be not null.");
        CommonUtils.checkArgument(buffers.length > 0, "No buffer to write.");

        long expectedBytes = 0;
        for (ByteBuffer buffer : buffers) {
            expectedBytes += buffer.remaining();
        }

        long bytesWritten = fileChannel.write(buffers);
        while (bytesWritten < expectedBytes) {
            int bufferOffset = 0;
            for (ByteBuffer buffer : buffers) {
                if (buffer.hasRemaining()) {
                    break;
                }
                ++bufferOffset;
            }
            bytesWritten += fileChannel.write(buffers, bufferOffset, buffers.length - bufferOffset);
        }
        return expectedBytes;
    }

    /**
     * Creates and returns the corresponding header {@link ByteBuffer} of the target data {@link
     * ByteBuffer}.
     */
    public static ByteBuffer getHeaderBuffer(ByteBuffer buffer, boolean dataChecksumEnabled) {
        CommonUtils.checkArgument(buffer != null, "Must be not null.");

        ByteBuffer header = allocateHeaderBuffer();
        header.putInt(buffer.remaining());
        if (!dataChecksumEnabled) {
            header.putInt(MAGIC_NUMBER_WITHOUT_CHECKSUM);
            header.putLong(0L);
        } else {
            header.putInt(MAGIC_NUMBER_WITH_CHECKSUM);
            Checksum checksum = new CRC32();
            for (int i = 0; i < buffer.remaining(); ++i) {
                checksum.update(buffer.get(i));
            }
            header.putLong(checksum.getValue());
        }
        header.flip();
        return header;
    }

    /**
     * Reads the target length of data from the given {@link FileChannel} to the target {@link
     * ByteBuffer}.
     */
    public static long readBuffer(FileChannel fileChannel, ByteBuffer buffer, int length)
            throws IOException {
        CommonUtils.checkArgument(length <= buffer.capacity(), "Too many bytes to read.");

        long remainingBytes = fileChannel.size() - fileChannel.position();
        if (remainingBytes < length) {
            LOG.error(
                    String.format(
                            "File remaining bytes not not enough, remaining: %d, wanted: %d.",
                            remainingBytes, length));
            throw new FileCorruptedException();
        }

        buffer.clear();
        buffer.limit(length);

        while (buffer.hasRemaining()) {
            fileChannel.read(buffer);
        }
        buffer.flip();
        return buffer.remaining();
    }

    /**
     * Reads data with header from the given {@link FileChannel} to the target {@link ByteBuffer}.
     */
    public static int readBuffer(
            FileChannel fileChannel,
            ByteBuffer header,
            ByteBuffer buffer,
            int headerSize,
            boolean dataChecksumEnabled)
            throws IOException {
        CommonUtils.checkArgument(fileChannel != null, "Must be not null.");
        CommonUtils.checkArgument(header != null, "Must be not null.");
        CommonUtils.checkArgument(buffer != null, "Must be not null.");
        CommonUtils.checkArgument(header.capacity() >= headerSize, "Illegal header buffer.");

        readBuffer(fileChannel, header, headerSize);
        int bufferLength = header.getInt();
        int magicNumber = header.getInt();
        if ((magicNumber != MAGIC_NUMBER_WITHOUT_CHECKSUM
                        && magicNumber != MAGIC_NUMBER_WITH_CHECKSUM)
                || bufferLength <= 0
                || bufferLength > buffer.capacity()) {
            LOG.error(
                    String.format(
                            "Incorrect buffer header, magic number: %d, buffer length: %d.",
                            magicNumber, bufferLength));
            throw new FileCorruptedException();
        }

        readBuffer(fileChannel, buffer, bufferLength);
        if (dataChecksumEnabled && magicNumber == MAGIC_NUMBER_WITH_CHECKSUM) {
            Checksum checksum = new CRC32();
            for (int i = 0; i < buffer.remaining(); ++i) {
                checksum.update(buffer.get(i));
            }
            if (checksum.getValue() != header.getLong()) {
                LOG.error("Data checksum verification failed.");
                throw new FileCorruptedException();
            }
        }
        return bufferLength + headerSize;
    }

    /**
     * Allocates a piece of unmanaged direct {@link ByteBuffer} as header buffer which can be reused
     * multiple times.
     */
    public static ByteBuffer allocateHeaderBuffer() {
        return CommonUtils.allocateDirectByteBuffer(HEADER_BUFFER_SIZE);
    }

    /**
     * Allocates a piece of unmanaged direct {@link ByteBuffer} for index data writing/reading. The
     * minimum index buffer size returned is 4096 bytes.
     */
    public static ByteBuffer allocateIndexBuffer(int numPartitions) {
        CommonUtils.checkArgument(numPartitions > 0, "Must be positive.");

        // the returned buffer size is no smaller than 4096 bytes to improve disk IO performance
        int minBufferSize = 4096;

        int indexRegionSize = calculateIndexRegionSize(numPartitions);
        if (indexRegionSize >= minBufferSize) {
            return CommonUtils.allocateDirectByteBuffer(indexRegionSize);
        }

        int numRegions = minBufferSize / indexRegionSize;
        if (minBufferSize % indexRegionSize != 0) {
            ++numRegions;
        }
        return CommonUtils.allocateDirectByteBuffer(numRegions * indexRegionSize);
    }

    /**
     * Allocates a piece of unmanaged direct {@link ByteBuffer} for index data checksum writing and
     * reading.
     */
    public static ByteBuffer allocateIndexDataChecksumBuffer() {
        return CommonUtils.allocateDirectByteBuffer(LocalMapPartitionFile.INDEX_DATA_CHECKSUM_SIZE);
    }

    /** Calculates and returns the size of index region in bytes. */
    public static int calculateIndexRegionSize(int numPartitions) {
        return CommonUtils.checkedDownCast(
                (long) numPartitions * LocalMapPartitionFile.INDEX_ENTRY_SIZE);
    }

    /** Return the header buffer size. For different versions, this value may be different. */
    public static int getHeaderBufferSizeOfLocalMapPartitionFile(int storageVersion) {
        switch (storageVersion) {
            case 0:
                return 8;
            case 1:
                return 16;
            default:
                throw new IllegalArgumentException("Unknown storage version: " + storageVersion);
        }
    }
}
