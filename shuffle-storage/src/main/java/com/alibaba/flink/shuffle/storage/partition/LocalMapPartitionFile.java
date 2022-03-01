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
import com.alibaba.flink.shuffle.storage.exception.FileCorruptedException;
import com.alibaba.flink.shuffle.storage.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

/**
 * Local file based {@link PersistentFile} implementation which is used to store data partition data
 * to and read data partition data from local file. It consists of two files, the data file is used
 * to store data and the index file is used to store index information of the data file for reading.
 *
 * <p>The data file contains one or multiple sequential data regions. <b>In each data region, data
 * of different reduce partitions will be stored in reduce partition index order</b>. In the index
 * file, for each data region in the data file, there will be an index region. Each index region
 * contains the same number of index entries with the number of reduce partitions and each index
 * entry point to the data of the corresponding reduce partition in the data file.
 */
@NotThreadSafe
public class LocalMapPartitionFile implements PersistentFile {

    private static final Logger LOG = LoggerFactory.getLogger(LocalMapPartitionFile.class);

    /**
     * Latest storage version used. One need to increase this version after changing the storage
     * format.
     */
    public static final int LATEST_STORAGE_VERSION = 1;

    /**
     * Size of each index entry in the index file: 8 for file offset, 8 for number of bytes and 4
     * bytes for magic number.
     */
    public static final int INDEX_ENTRY_SIZE = 8 + 8 + 4;

    /**
     * Size of index data checksum, 8 for the number of data regions and 8 for magic number. The
     * index data checksum sits at the tail the index data file.
     */
    public static final int INDEX_DATA_CHECKSUM_SIZE = 8 + 8;

    /** Name suffix of the data file. */
    public static final String DATA_FILE_SUFFIX = ".data";

    /** Name suffix of the index file. */
    public static final String INDEX_FILE_SUFFIX = ".index";

    /** Name suffix of the in-progressing partial data file. */
    public static final String PARTIAL_DATA_FILE_SUFFIX = ".data.partial";

    /** Name suffix of the in-progressing partial index file. */
    public static final String PARTIAL_INDEX_FILE_SUFFIX = ".index.partial";

    /** Meta information of this data partition file. */
    private final LocalMapPartitionFileMeta fileMeta;

    /**
     * Maximum number of {@link IOException}s can be tolerated before marking this partition file as
     * corrupted.
     */
    private final int tolerableFailures;

    /** All data readers that have opened and are reading data from the file. */
    private final Set<Object> readers = new HashSet<>();

    /** Opened data file channel shared by all data readers for data reading. */
    @Nullable private FileChannel dataReadingChannel;

    /** Opened index file channel shared by all data readers for data reading. */
    @Nullable private FileChannel indexReadingChannel;

    /** Counter recording all data reading failures. */
    private int failureCounter;

    /** Whether this persistent file is consumable and ready for data reading. */
    private volatile boolean isConsumable;

    /** Statistics information of this persistent file. */
    private volatile PersistentFileStatistics statistics;

    /** Whether the checksum of the corresponding index data is verified or not. */
    private volatile boolean indexDataChecksumVerified;

    public LocalMapPartitionFile(
            LocalMapPartitionFileMeta fileMeta,
            int tolerableFailures,
            boolean indexDataChecksumVerified) {
        this(
                fileMeta,
                tolerableFailures,
                indexDataChecksumVerified,
                new PersistentFileStatistics(0, 0, 0));
    }

    public LocalMapPartitionFile(
            LocalMapPartitionFileMeta fileMeta,
            int tolerableFailures,
            boolean indexDataChecksumVerified,
            PersistentFileStatistics statistics) {
        CommonUtils.checkArgument(fileMeta != null, "Must be not null.");
        CommonUtils.checkArgument(tolerableFailures >= 0, "Must be non-negative.");
        CommonUtils.checkArgument(statistics != null, "Must be not null.");

        this.fileMeta = fileMeta;
        this.tolerableFailures = tolerableFailures;
        this.indexDataChecksumVerified = indexDataChecksumVerified;
        this.statistics = statistics;
    }

    @Override
    public int getLatestStorageVersion() {
        return LATEST_STORAGE_VERSION;
    }

    @Override
    public boolean isConsumable() {
        return isConsumable
                && Files.isReadable(fileMeta.getDataFilePath())
                && Files.isReadable(fileMeta.getIndexFilePath());
    }

    @Override
    public void updatePersistentFileStatistics(PersistentFileStatistics statistics) {
        this.statistics = statistics;
    }

    @Override
    public PersistentFileStatistics getPersistentFileStatistics() {
        return statistics;
    }

    @Override
    public LocalMapPartitionFileMeta getFileMeta() {
        return fileMeta;
    }

    /**
     * Opens this file for reading. This method maintains the reader set and will only open the new
     * file channels when the first reader is registered. It guarantees that all opened resources
     * will be released if any exception occurs.
     */
    public void openFile(Object reader) throws Exception {
        CommonUtils.checkArgument(reader != null, "Must be not null.");
        CommonUtils.checkState(fileMeta.getStorageVersion() <= 1, "Illegal storage version.");

        if (!readers.isEmpty()) {
            readers.add(reader);
            return;
        }

        if (!indexDataChecksumVerified) {
            verifyIndexDataChecksum();
            indexDataChecksumVerified = true;
        }

        try {
            Path dataFilePath = fileMeta.getDataFilePath();
            dataReadingChannel = IOUtils.openReadableFileChannel(dataFilePath);
            Path indexFilePath = fileMeta.getIndexFilePath();
            indexReadingChannel = IOUtils.openReadableFileChannel(indexFilePath);

            readers.add(reader);
        } catch (Throwable throwable) {
            CommonUtils.runQuietly(this::closeFileChannels);
            LOG.error("Failed to open the partition for reading: {}.", fileMeta.getFilePath());
            throw throwable;
        }
    }

    /**
     * Marks the given file reader as closed. This method maintains the reader set and will only
     * close the opened file channels when all file readers have been closed.
     */
    public void closeFile(Object reader) throws Exception {
        CommonUtils.checkArgument(reader != null, "Must be not null.");

        readers.remove(reader);
        if (readers.isEmpty()) {
            closeFileChannels();
        }
    }

    @Override
    public void deleteFile() throws Exception {
        LOG.info("Deleting the partition file: {}.", fileMeta.getFilePath());
        Throwable exception = null;

        try {
            // close file channel first if opened
            closeFileChannels();
        } catch (Throwable throwable) {
            exception = throwable;
        }

        Path filePath = fileMeta.getDataFilePath();
        try {
            CommonUtils.deleteFileWithRetry(filePath);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Failed to delete data file: {}.", filePath, throwable);
        }

        filePath = fileMeta.getIndexFilePath();
        try {
            CommonUtils.deleteFileWithRetry(filePath);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Failed to delete index file: {}.", filePath, throwable);
        }

        filePath = fileMeta.getPartialDataFilePath();
        try {
            CommonUtils.deleteFileWithRetry(filePath);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Failed to delete data file: {}.", filePath, throwable);
        }

        filePath = fileMeta.getPartialIndexFilePath();
        try {
            CommonUtils.deleteFileWithRetry(filePath);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Failed to delete index file: {}.", filePath, throwable);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if (throwable instanceof ClosedChannelException) {
            return;
        }

        if (throwable instanceof FileCorruptedException
                || (throwable instanceof IOException && ++failureCounter > tolerableFailures)) {
            // to trigger partition not found exception
            setConsumable(false);
            LOG.error("Corrupted partition file: {}.", fileMeta.getFilePath(), throwable);
        }
    }

    @Override
    public void setConsumable(boolean isConsumable) {
        this.isConsumable = isConsumable;

        // delete the physical files eagerly
        if (!isConsumable) {
            CommonUtils.runQuietly(this::closeFileChannels);
            CommonUtils.runQuietly(this::deleteFile);
        }
    }

    @Nullable
    public FileChannel getDataReadingChannel() {
        return dataReadingChannel;
    }

    @Nullable
    public FileChannel getIndexReadingChannel() {
        return indexReadingChannel;
    }

    /** Returns the total size of all index entries for a data region. */
    public long getIndexRegionSize() {
        return fileMeta.getNumReducePartitions() * (long) INDEX_ENTRY_SIZE;
    }

    /** Returns the offset in the index file of the target index entry. */
    public long getIndexEntryOffset(int regionIndex, int targetPartitionIndex) {
        return regionIndex * getIndexRegionSize() + targetPartitionIndex * (long) INDEX_ENTRY_SIZE;
    }

    public static Path getDataFilePath(String baseFilePath) {
        CommonUtils.checkArgument(baseFilePath != null, "Must be not null.");

        return new File(baseFilePath + DATA_FILE_SUFFIX).toPath();
    }

    public static Path getIndexFilePath(String baseFilePath) {
        CommonUtils.checkArgument(baseFilePath != null, "Must be not null.");

        return new File(baseFilePath + INDEX_FILE_SUFFIX).toPath();
    }

    public static Path getPartialDataFilePath(String baseFilePath) {
        CommonUtils.checkArgument(baseFilePath != null, "Must be not null.");

        return new File(baseFilePath + PARTIAL_DATA_FILE_SUFFIX).toPath();
    }

    public static Path getPartialIndexFilePath(String baseFilePath) {
        CommonUtils.checkArgument(baseFilePath != null, "Must be not null.");

        return new File(baseFilePath + PARTIAL_INDEX_FILE_SUFFIX).toPath();
    }

    /** Closes the opened data file channel and index file channel. */
    private void closeFileChannels() throws Exception {
        Throwable exception = null;
        try {
            CommonUtils.closeWithRetry(dataReadingChannel);
            dataReadingChannel = null;
        } catch (Throwable throwable) {
            exception = throwable;
            LOG.error(
                    "Failed to close data file channel: {}.",
                    fileMeta.getDataFilePath(),
                    throwable);
        }

        try {
            CommonUtils.closeWithRetry(indexReadingChannel);
            indexReadingChannel = null;
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error(
                    "Failed to close index file channel: {}.",
                    fileMeta.getIndexFilePath(),
                    throwable);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    private void verifyIndexDataChecksum() throws Exception {
        FileChannel fileChannel = null;
        try {
            fileChannel = IOUtils.openReadableFileChannel(fileMeta.getIndexFilePath());
            int numPartitions = fileMeta.getNumReducePartitions();
            int indexRegionSize = IOUtils.calculateIndexRegionSize(numPartitions);

            long remainingDataSize = fileChannel.size() - INDEX_DATA_CHECKSUM_SIZE;
            if (remainingDataSize % indexRegionSize != 0) {
                throw new FileCorruptedException();
            }

            Checksum checksum = new CRC32();
            int numRegions = CommonUtils.checkedDownCast(remainingDataSize / indexRegionSize);
            ByteBuffer indexBuffer = IOUtils.allocateIndexBuffer(numPartitions);

            while (remainingDataSize > 0) {
                long length = Math.min(remainingDataSize, indexBuffer.capacity());
                IOUtils.readBuffer(fileChannel, indexBuffer, CommonUtils.checkedDownCast(length));
                remainingDataSize -= length;
                while (indexBuffer.hasRemaining()) {
                    checksum.update(indexBuffer.get());
                }
            }

            IOUtils.readBuffer(fileChannel, indexBuffer, INDEX_DATA_CHECKSUM_SIZE);
            if (numRegions != indexBuffer.getLong()
                    || checksum.getValue() != indexBuffer.getLong()) {
                throw new FileCorruptedException();
            }
        } catch (Throwable throwable) {
            setConsumable(false);
            LOG.error("Failed to verify index data checksum, releasing partition data.", throwable);
            throw throwable;
        } finally {
            CommonUtils.closeWithRetry(fileChannel);
        }
    }
}
