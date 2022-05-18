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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.storage.exception.FileCorruptedException;
import com.alibaba.flink.shuffle.storage.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

/**
 * File reader for the {@link LocalMapPartitionFile}. Each {@link LocalMapPartitionFileReader} only
 * reads the data belonging to the target reduce partition range ({@link #startPartitionIndex} to
 * {@link #endPartitionIndex}).
 */
@NotThreadSafe
public class LocalMapPartitionFileReader {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMapPartitionFileReader.class);

    /** Buffer for reading an index entry to memory. */
    private final ByteBuffer indexBuffer;

    /** Buffer for reading buffer header to memory. */
    private final ByteBuffer headerBuffer;

    /** Target partition file to be read data from. */
    private final LocalMapPartitionFile partitionFile;

    /** First index (inclusive) of the target reduce partitions to be read. */
    private final int startPartitionIndex;

    /** Last index (inclusive) of the target reduce partitions to be read. */
    private final int endPartitionIndex;

    /** Whether to enable data checksum or not. */
    private final boolean dataChecksumEnabled;

    /** Number of data regions in the target file. */
    private int numRegions;

    /** Opened data file channel to read data from. */
    private FileChannel dataFileChannel;

    /** Opened index file channel to read index from. */
    private FileChannel indexFileChannel;

    /** Number of remaining reduce partitions to read in the current data region. */
    private int numRemainingPartitions;

    /** Current data region index to read data from. */
    private int currentDataRegion = -1;

    /** File offset in data file to read data from. */
    private long dataConsumingOffset;

    /** Number of remaining bytes to read from the current reduce partition. */
    private long currentPartitionRemainingBytes;

    /** Whether this file reader is closed or not. A closed file reader can not read any data. */
    private boolean isClosed;

    /** Whether this file reader is opened or not. */
    private boolean isOpened;

    public LocalMapPartitionFileReader(
            boolean dataChecksumEnabled,
            int startPartitionIndex,
            int endPartitionIndex,
            LocalMapPartitionFile partitionFile) {
        CommonUtils.checkArgument(partitionFile != null, "Must be not null.");
        CommonUtils.checkArgument(startPartitionIndex >= 0, "Must be non-negative.");
        CommonUtils.checkArgument(
                endPartitionIndex >= startPartitionIndex,
                "Ending partition index must be no smaller than starting partition index.");
        CommonUtils.checkState(
                endPartitionIndex < partitionFile.getFileMeta().getNumReducePartitions(),
                "Ending partition index must be smaller than number of reduce partitions.");

        this.partitionFile = partitionFile;
        this.startPartitionIndex = startPartitionIndex;
        this.endPartitionIndex = endPartitionIndex;
        this.dataChecksumEnabled = dataChecksumEnabled;

        int indexBufferSize =
                LocalMapPartitionFile.INDEX_ENTRY_SIZE
                        * (endPartitionIndex - startPartitionIndex + 1);
        this.indexBuffer = CommonUtils.allocateDirectByteBuffer(indexBufferSize);
        this.headerBuffer =
                CommonUtils.allocateDirectByteBuffer(
                        IOUtils.getHeaderBufferSizeOfLocalMapPartitionFile(
                                partitionFile.getFileMeta().getStorageVersion()));
    }

    public void open() throws Exception {
        CommonUtils.checkState(!isOpened, "Partition file reader has been opened.");
        CommonUtils.checkState(!isClosed, "Partition file reader has been closed.");

        try {
            isOpened = true;
            partitionFile.openFile(this);
            dataFileChannel = CommonUtils.checkNotNull(partitionFile.getDataReadingChannel());
            indexFileChannel = CommonUtils.checkNotNull(partitionFile.getIndexReadingChannel());

            long indexFileSize = indexFileChannel.size();
            long indexRegionSize = partitionFile.getIndexRegionSize();

            // if this checks fail, the partition file must be corrupted
            if (indexFileSize % indexRegionSize != LocalMapPartitionFile.INDEX_DATA_CHECKSUM_SIZE) {
                throw new FileCorruptedException();
            }

            numRegions = CommonUtils.checkedDownCast(indexFileChannel.size() / indexRegionSize);
            updateConsumingOffset();
        } catch (Throwable throwable) {
            CommonUtils.runQuietly(this::close);
            partitionFile.onError(throwable);

            LOG.debug("Failed to open partition file.", throwable);
            throw throwable;
        }
    }

    /**
     * Reads data to the target buffer and returns true if there is remaining data in current data
     * region. The caller is responsible for recycling the target buffer if any exception occurs.
     */
    public boolean readBuffer(ByteBuffer buffer) throws Exception {
        CommonUtils.checkArgument(buffer != null, "Must be not null.");

        CommonUtils.checkState(isOpened, "Partition file reader is not opened.");
        CommonUtils.checkState(!isClosed, "Partition file reader has been closed.");
        // one must check remaining before reading
        CommonUtils.checkState(hasRemaining(), "No remaining data to read.");

        try {
            dataFileChannel.position(dataConsumingOffset);
            currentPartitionRemainingBytes -=
                    IOUtils.readBuffer(
                            dataFileChannel,
                            headerBuffer,
                            buffer,
                            headerBuffer.capacity(),
                            dataChecksumEnabled);

            // if this check fails, the partition file must be corrupted
            if (currentPartitionRemainingBytes < 0) {
                throw new FileCorruptedException();
            } else if (currentPartitionRemainingBytes == 0) {
                int prevDataRegion = currentDataRegion;
                updateConsumingOffset();
                return prevDataRegion == currentDataRegion && currentPartitionRemainingBytes > 0;
            }

            dataConsumingOffset = dataFileChannel.position();
            return true;
        } catch (Throwable throwable) {
            partitionFile.onError(throwable);
            LOG.debug("Failed to read partition file.", throwable);
            throw throwable;
        }
    }

    /** Returns true if there is remaining bytes in the target partition file. */
    public boolean hasRemaining() {
        return currentPartitionRemainingBytes > 0;
    }

    /** Returns the next data reading offset in the target data partition file. */
    public long geConsumingOffset() {
        return dataConsumingOffset;
    }

    /** Updates to next data reading offset in the target data partition file. */
    private void updateConsumingOffset() throws IOException {
        while (currentPartitionRemainingBytes == 0
                && (currentDataRegion < numRegions - 1 || numRemainingPartitions > 0)) {
            if (numRemainingPartitions <= 0) {
                ++currentDataRegion;
                numRemainingPartitions = endPartitionIndex - startPartitionIndex + 1;

                // read the target index entry to the target index buffer
                indexFileChannel.position(
                        partitionFile.getIndexEntryOffset(currentDataRegion, startPartitionIndex));
                IOUtils.readBuffer(indexFileChannel, indexBuffer, indexBuffer.capacity());
            }

            // get the data file offset and the data size
            dataConsumingOffset = indexBuffer.getLong();
            currentPartitionRemainingBytes = indexBuffer.getLong();
            int magicNumber = indexBuffer.getInt();
            --numRemainingPartitions;

            // if these checks fail, the partition file must be corrupted
            if (dataConsumingOffset < 0
                    || dataConsumingOffset + currentPartitionRemainingBytes > dataFileChannel.size()
                    || currentPartitionRemainingBytes < 0
                    || magicNumber != IOUtils.MAGIC_NUMBER) {
                throw new FileCorruptedException();
            }
        }
    }

    public void finishReading() throws Exception {
        close();

        LocalMapPartitionFileMeta fileMeta = partitionFile.getFileMeta();
        CommonUtils.checkState(
                Files.exists(fileMeta.getDataFilePath()), "Data file has been deleted.");
        CommonUtils.checkState(
                Files.exists(fileMeta.getIndexFilePath()), "Index file has been deleted.");
    }

    /** Closes this partition file reader. After closed, no data can be read any more. */
    public void close() throws Exception {
        isClosed = true;
        partitionFile.closeFile(this);
    }

    public boolean isOpened() {
        return isOpened;
    }
}
