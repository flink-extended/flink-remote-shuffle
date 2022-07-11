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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.storage.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** File writer for the {@link LocalReducePartitionFile}. */
public class LocalReducePartitionFileWriter {

    private static final Logger LOG = LoggerFactory.getLogger(LocalReducePartitionFileWriter.class);

    /** Maximum number of data buffers can be cached in {@link #dataBuffers} before flushing. */
    private final int dataBufferCacheSize;

    /**
     * All pending {@link BufferOrMarker.DataBuffer}s to be written. This list is for batch writing
     * which can be better for IO performance.
     */
    protected final List<BufferOrMarker.DataBuffer> dataBuffers;

    protected final Set<MapPartitionID> regionStartMapIDs;

    /** Target {@link LocalReducePartitionFile} to write data to. */
    private final LocalReducePartitionFile partitionFile;

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

    private boolean isCachingData;

    /**
     * Checksum util to calculate the checksum value the index data. The completeness of index data
     * is important because it is used to index the real data. The lost of index data just means the
     * lost of the real data.
     */
    private final Checksum checksum = new CRC32();

    /** Whether to enable data checksum or not. */
    private final boolean dataChecksumEnabled;

    public LocalReducePartitionFileWriter(
            LocalReducePartitionFile partitionFile,
            int dataBufferCacheSize,
            boolean dataChecksumEnabled) {
        CommonUtils.checkArgument(partitionFile != null, "Must be not null.");
        CommonUtils.checkArgument(dataBufferCacheSize > 0, "Must be positive.");

        this.partitionFile = partitionFile;
        this.dataBufferCacheSize = dataBufferCacheSize;
        this.dataBuffers = new ArrayList<>(2 * dataBufferCacheSize);
        this.regionStartMapIDs = new HashSet<>();
        this.dataChecksumEnabled = dataChecksumEnabled;

        this.indexBuffer = IOUtils.allocateIndexBuffer(1);
        LOG.debug("dataBufferCacheSize={}", dataBufferCacheSize);
    }

    public void open() throws Exception {
        checkState(!isOpened, "Partition file writer has been opened.");
        checkState(!isClosed, "Partition file writer has been closed.");

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
     * LocalReducePartitionFile}.
     */
    public void writeBuffer(BufferOrMarker.DataBuffer dataBuffer, boolean isLastBufferOrMarker)
            throws IOException {
        CommonUtils.checkArgument(dataBuffer != null, "Must be not null.");

        checkState(isOpened, "Partition file writer is not opened.");
        checkState(!isClosed, "Partition file writer has been closed.");

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

    void flushDataBuffers() throws IOException {
        try {
            checkNotClosed();
            isCachingData = false;

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
            checkState(
                    reducePartitionIndex >= currentReducePartition,
                    "Must writing data in reduce partition index order.");
            checkState(
                    !isBroadcastRegion || reducePartitionIndex == 0,
                    "Reduce partition index must be 0 for broadcast region.");

            if (reducePartitionIndex > currentReducePartition) {
                currentReducePartition = reducePartitionIndex;
            }

            ByteBuffer data = dataBuffer.getBuffer().nioBuffer();
            ByteBuffer header = IOUtils.getHeaderBuffer(data, dataChecksumEnabled);

            long length = data.remaining() + header.remaining();
            totalBytes += length;

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
    public void startRegion(boolean isBroadcastRegion, MapPartitionID mapPartitionID) {
        checkNotClosed();
        currentReducePartition = Math.max(currentReducePartition, 0);
        this.isBroadcastRegion = isBroadcastRegion;
        regionStartMapIDs.add(mapPartitionID);
    }

    /**
     * Marks that the current data region has been finished and flushes the index region to the
     * index file.
     */
    public void finishRegion(MapPartitionID mapPartitionID) throws IOException {
        checkNotClosed();
        checkState(
                regionStartMapIDs.contains(mapPartitionID),
                "No region start message was received for " + mapPartitionID);

        if (dataBuffers.size() >= dataBufferCacheSize) {
            flushDataBuffers();
        } else {
            isCachingData = true;
        }

        ++numDataRegions;
        regionStartMapIDs.remove(mapPartitionID);
    }

    private void flushIndexBuffer() throws IOException {
        indexBuffer.flip();
        if (indexBuffer.hasRemaining()) {
            for (int index = 0; index < indexBuffer.limit(); ++index) {
                checksum.update(indexBuffer.get(index));
            }
            IOUtils.writeBuffer(indexFileChannel, indexBuffer);
        }
        indexBuffer.clear();
    }

    boolean isCachingData() {
        return isCachingData;
    }

    public void prepareFinishWriting(BufferOrMarker.InputFinishedMarker marker) throws Exception {
        if (!isOpened()) {
            open();
        }

        checkNotClosed();
        flushDataBuffers();
    }

    /**
     * Closes this partition file writer and marks the target {@link LocalReducePartitionFile} as
     * consumable after finishing data writing.
     */
    public void closeWriting() throws Exception {
        checkState(regionStartMapIDs.size() == 0, "Wrong region finish marker count. ");
        checkState(dataBuffers.size() == 0, "Must flush cached data firstly. ");

        indexBuffer.putLong(0);
        indexBuffer.putLong(totalBytes);
        indexBuffer.putInt(IOUtils.MAGIC_NUMBER);

        flushIndexBuffer();

        // flush the number of data regions and the index data checksum for integrity checking
        indexBuffer.putLong(numDataRegions);
        indexBuffer.putLong(checksum.getValue());
        indexBuffer.flip();
        IOUtils.writeBuffer(indexFileChannel, indexBuffer);

        close();

        LocalReducePartitionFileMeta fileMeta = partitionFile.getFileMeta();
        File dataFile = fileMeta.getDataFilePath().toFile();
        renameFile(fileMeta.getPartialDataFilePath().toFile(), dataFile);

        File indexFile = fileMeta.getIndexFilePath().toFile();
        renameFile(fileMeta.getPartialIndexFilePath().toFile(), indexFile);

        checkState(dataFile.exists(), "Data file has been deleted.");
        checkState(indexFile.exists(), "Index file has been deleted.");
        partitionFile.setConsumable(true);
        LOG.info("Closed file for {} and {}, total {} bytes", dataFile, indexFile, totalBytes);
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

    public int numCacheDataBuffers() {
        return dataBuffers.size();
    }

    public int numDataBufferCacheSize() {
        return dataBufferCacheSize;
    }

    public boolean isOpened() {
        return isOpened;
    }

    protected boolean isClosed() {
        return isClosed;
    }

    private void releaseAllDataBuffers() {
        for (BufferOrMarker.DataBuffer dataBuffer : dataBuffers) {
            BufferOrMarker.releaseBuffer(dataBuffer);
        }
        dataBuffers.clear();
    }

    private void checkNotClosed() {
        checkState(!isClosed, "Partition file writer has been closed.");
    }
}
