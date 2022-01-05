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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.storage.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/** File writer for the {@link LocalMapPartitionFile}. */
public class LocalMapPartitionFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMapPartitionFileWriter.class);

    /** Maximum number of data buffers can be cached in {@link #dataBuffers} before flushing. */
    private final int dataBufferCacheSize;

    /**
     * All pending {@link BufferOrMarker.DataBuffer}s to be written. This list is for batch writing
     * which can be better for IO performance.
     */
    protected final List<BufferOrMarker.DataBuffer> dataBuffers;

    /** Target {@link LocalMapPartitionFile} to write data to. */
    private final LocalMapPartitionFile partitionFile;

    /** Number of bytes of all reduce partitions in the current data region. */
    private final long[] numReducePartitionBytes;

    /** Caches the index data before flushing the data to target index file. */
    private final ByteBuffer indexBuffer;

    /** Opened data file channel to write data buffers to. */
    private FileChannel dataFileChannel;

    /** Opened index file channel to write index info to. */
    private FileChannel indexFileChannel;

    /** Current reduce partition index to which the data buffer is written. */
    private int currentReducePartition;

    /** Total bytes of data have been written to the target partition file. */
    private long totalBytes;

    /** Staring offset in the target data file of the current data region. */
    private long regionStartingOffset;

    /**
     * Whether current data region is a broadcast region or not. If true, buffers added to this
     * region will be written to all reduce partitions.
     */
    private boolean isBroadcastRegion;

    /** Whether this file writer has been closed or not. */
    private boolean isClosed;

    /** Whether this file writer has been opened or not. */
    private boolean isOpened;

    /** Number of finished data regions in the target {@link LocalMapPartitionFile} currently. */
    private long numDataRegions;

    /**
     * Checksum util to calculate the checksum value the index data. The completeness of index data
     * is important because it is used to index the real data. The lost of index data just means the
     * lost of the real data.
     */
    private final Checksum checksum = new CRC32();

    /** Whether to enable data checksum or not. */
    private final boolean dataChecksumEnabled;

    public LocalMapPartitionFileWriter(
            LocalMapPartitionFile partitionFile,
            int dataBufferCacheSize,
            boolean dataChecksumEnabled) {
        CommonUtils.checkArgument(partitionFile != null, "Must be not null.");
        CommonUtils.checkArgument(dataBufferCacheSize > 0, "Must be positive.");

        this.partitionFile = partitionFile;
        this.dataBufferCacheSize = dataBufferCacheSize;
        this.dataBuffers = new ArrayList<>(2 * dataBufferCacheSize);
        this.dataChecksumEnabled = dataChecksumEnabled;

        LocalMapPartitionFileMeta fileMeta = partitionFile.getFileMeta();
        int numReducePartitions = fileMeta.getNumReducePartitions();
        this.numReducePartitionBytes = new long[numReducePartitions];
        this.indexBuffer = IOUtils.allocateIndexBuffer(numReducePartitions);
    }

    public void open() throws Exception {
        CommonUtils.checkState(!isOpened, "Partition file writer has been opened.");
        CommonUtils.checkState(!isClosed, "Partition file writer has been closed.");

        try {
            isOpened = true;
            Path dataFilePath = partitionFile.getFileMeta().getPartialDataFilePath();
            dataFileChannel = IOUtils.createWritableFileChannel(dataFilePath);

            Path indexFilePath = partitionFile.getFileMeta().getPartialIndexFilePath();
            indexFileChannel = IOUtils.createWritableFileChannel(indexFilePath);
        } catch (Throwable throwable) {
            CommonUtils.runQuietly(this::close);
            throw throwable;
        }
    }

    /**
     * Writes the given data buffer of the corresponding reduce partition to the target {@link
     * LocalMapPartitionFile}.
     */
    public void writeBuffer(BufferOrMarker.DataBuffer dataBuffer) throws IOException {
        CommonUtils.checkArgument(dataBuffer != null, "Must be not null.");

        CommonUtils.checkState(isOpened, "Partition file writer is not opened.");
        CommonUtils.checkState(!isClosed, "Partition file writer has been closed.");

        if (!dataBuffer.getBuffer().isReadable()) {
            dataBuffer.release();
            return;
        }

        dataBuffers.add(dataBuffer);
        if (dataBuffers.size() >= dataBufferCacheSize) {
            flushDataBuffers();
            CommonUtils.checkState(
                    dataBuffers.isEmpty(),
                    "Leaking buffers, some buffers are not released after flush.");
        }
    }

    private void flushDataBuffers() throws IOException {
        try {
            checkNotClosed();

            if (!dataBuffers.isEmpty()) {
                ByteBuffer[] bufferWithHeaders = collectBufferWithHeaders();
                IOUtils.writeBuffers(dataFileChannel, bufferWithHeaders);
            }
        } finally {
            releaseAllDataBuffers();
        }
    }

    private ByteBuffer[] collectBufferWithHeaders() {
        int index = 0;
        ByteBuffer[] bufferWithHeaders = new ByteBuffer[2 * dataBuffers.size()];

        for (BufferOrMarker.DataBuffer dataBuffer : dataBuffers) {
            int reducePartitionIndex = dataBuffer.getReducePartitionID().getPartitionIndex();
            CommonUtils.checkState(
                    reducePartitionIndex >= currentReducePartition,
                    "Must writing data in reduce partition index order.");
            CommonUtils.checkState(
                    !isBroadcastRegion || reducePartitionIndex == 0,
                    "Reduce partition index must be 0 for broadcast region.");

            if (reducePartitionIndex > currentReducePartition) {
                currentReducePartition = reducePartitionIndex;
            }

            ByteBuffer data = dataBuffer.getBuffer().nioBuffer();
            ByteBuffer header = IOUtils.getHeaderBuffer(data, dataChecksumEnabled);

            long length = data.remaining() + header.remaining();
            totalBytes += length;
            partitionFile.incrementTotalBytes(length);
            numReducePartitionBytes[reducePartitionIndex] += length;

            bufferWithHeaders[index] = header;
            bufferWithHeaders[index + 1] = data;
            index += 2;
        }
        return bufferWithHeaders;
    }

    /**
     * Marks that a new data region has been started. If the new data region is a broadcast region,
     * buffers added to this region will be written to all reduce partitions.
     */
    public void startRegion(boolean isBroadcastRegion) {
        checkNotClosed();
        checkRegionFinished();

        currentReducePartition = 0;
        this.isBroadcastRegion = isBroadcastRegion;
    }

    /**
     * Marks that the current data region has been finished and flushes the index region to the
     * index file.
     */
    public void finishRegion() throws IOException {
        checkNotClosed();

        flushDataBuffers();
        if (regionStartingOffset == totalBytes) {
            return;
        }

        int numReducePartitions = partitionFile.getFileMeta().getNumReducePartitions();
        long fileOffset = regionStartingOffset;

        // write the index information of the current data region
        for (int partitionIndex = 0; partitionIndex < numReducePartitions; ++partitionIndex) {
            indexBuffer.putLong(fileOffset);
            if (!isBroadcastRegion) {
                indexBuffer.putLong(numReducePartitionBytes[partitionIndex]);
                fileOffset += numReducePartitionBytes[partitionIndex];
            } else {
                indexBuffer.putLong(numReducePartitionBytes[0]);
            }
            indexBuffer.putInt(IOUtils.MAGIC_NUMBER);
        }

        if (!indexBuffer.hasRemaining()) {
            flushIndexBuffer();
        }

        ++numDataRegions;
        regionStartingOffset = totalBytes;
        Arrays.fill(numReducePartitionBytes, 0);
    }

    private void flushIndexBuffer() throws IOException {
        indexBuffer.flip();
        if (indexBuffer.hasRemaining()) {
            for (int index = 0; index < indexBuffer.limit(); ++index) {
                checksum.update(indexBuffer.get(index));
            }
            partitionFile.incrementTotalBytes(indexBuffer.remaining());
            IOUtils.writeBuffer(indexFileChannel, indexBuffer);
        }
        indexBuffer.clear();
    }

    /**
     * Closes this partition file writer and marks the target {@link LocalMapPartitionFile} as
     * consumable after finishing data writing.
     */
    public void finishWriting() throws Exception {
        // handle empty data file
        if (!isOpened()) {
            open();
        }

        checkNotClosed();
        checkRegionFinished();

        flushIndexBuffer();

        // flush the number of data regions and the index data checksum for integrity checking
        indexBuffer.putLong(numDataRegions);
        indexBuffer.putLong(checksum.getValue());
        indexBuffer.flip();
        IOUtils.writeBuffer(indexFileChannel, indexBuffer);
        close();

        LocalMapPartitionFileMeta fileMeta = partitionFile.getFileMeta();
        File dataFile = fileMeta.getDataFilePath().toFile();
        renameFile(fileMeta.getPartialDataFilePath().toFile(), dataFile);

        File indexFile = fileMeta.getIndexFilePath().toFile();
        renameFile(fileMeta.getPartialIndexFilePath().toFile(), indexFile);

        CommonUtils.checkState(dataFile.exists(), "Data file has been deleted.");
        CommonUtils.checkState(indexFile.exists(), "Index file has been deleted.");
        partitionFile.setConsumable(true);
    }

    private void renameFile(File sourceFile, File targetFile) throws IOException {
        CommonUtils.checkArgument(sourceFile != null, "Must be not null.");
        CommonUtils.checkArgument(targetFile != null, "Must be not null.");

        if (!sourceFile.renameTo(targetFile)) {
            throw new IOException(
                    String.format(
                            "Failed to rename file %s to file %s.",
                            sourceFile.getAbsolutePath(), targetFile.getAbsolutePath()));
        }
    }

    /** Releases this partition file writer when any exception occurs. */
    public void close() throws Exception {
        isClosed = true;
        Throwable exception = null;

        try {
            CommonUtils.closeWithRetry(dataFileChannel);
        } catch (Throwable throwable) {
            exception = throwable;
            Path dataFilePath = partitionFile.getFileMeta().getDataFilePath();
            LOG.error("Failed to close data file channel: {}.", dataFilePath, throwable);
        }

        try {
            CommonUtils.closeWithRetry(indexFileChannel);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            Path dataFilePath = partitionFile.getFileMeta().getIndexFilePath();
            LOG.error("Failed to close index file channel: {}.", dataFilePath, throwable);
        }

        try {
            releaseAllDataBuffers();
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Failed to release the pending data buffers.", throwable);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    public boolean isOpened() {
        return isOpened;
    }

    private void releaseAllDataBuffers() {
        for (BufferOrMarker.DataBuffer dataBuffer : dataBuffers) {
            BufferOrMarker.releaseBuffer(dataBuffer);
        }
        dataBuffers.clear();
    }

    private void checkRegionFinished() {
        CommonUtils.checkState(
                regionStartingOffset == totalBytes,
                "Must finish the current data region before starting a new one.");
    }

    private void checkNotClosed() {
        CommonUtils.checkState(!isClosed, "Partition file writer has been closed.");
    }
}
