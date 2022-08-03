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
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;

/**
 * A {@link SortBuffer} implementation which sorts all appended records only by subpartition index.
 * Records of the same subpartition keep the appended order.
 *
 * <p>It maintains a list of {@link MemorySegment}s as a joint buffer. Data will be appended to the
 * joint buffer sequentially. When writing a record, an index entry will be appended first. An index
 * entry consists of 4 fields: 4 bytes for record length, 4 bytes for {@link DataType} and 8 bytes
 * for address pointing to the next index entry of the same channel which will be used to index the
 * next record to read when coping data from this {@link SortBuffer}. For simplicity, no index entry
 * can span multiple segments. The corresponding record data is seated right after its index entry
 * and different from the index entry, records have variable length thus may span multiple segments.
 */
@NotThreadSafe
public class PartitionSortedBuffer implements SortBuffer {

    /**
     * Size of an index entry: 4 bytes for record length, 4 bytes for data type and 8 bytes for
     * pointer to next entry.
     */
    private static final int INDEX_ENTRY_SIZE = 4 + 4 + 8;

    /** A flag to indicate the read process is finished. */
    private static final int READ_CHANNEL_FINISHED = 1;

    private final Object lock;
    /** A buffer pool to request memory segments from. */
    private final BufferPool bufferPool;

    /** A segment list as a joint buffer which stores all records and index entries. */
    @GuardedBy("lock")
    private final ArrayList<MemorySegment> buffers = new ArrayList<>();

    /** Addresses of the first record's index entry for each subpartition. */
    private final long[] firstIndexEntryAddresses;

    /** Addresses of the last record's index entry for each subpartition. */
    private final long[] lastIndexEntryAddresses;
    /** Size of buffers requested from buffer pool. All buffers must be of the same size. */
    private final int bufferSize;
    /** Data of different subpartitions in this sort buffer will be read in this order. */
    private final int[] subpartitionReadOrder;

    // ---------------------------------------------------------------------------------------------
    // Statistics and states
    // ---------------------------------------------------------------------------------------------
    /** Total number of bytes already appended to this sort buffer. */
    private long numTotalBytes;
    /** Total number of records already appended to this sort buffer. */
    private long numTotalRecords;
    /** Total number of bytes already read from this sort buffer. */
    private long numTotalBytesRead;
    /** Whether this sort buffer is finished. One can only read a finished sort buffer. */
    private boolean isFinished;
    /** Total number of bytes for each sub partition. */
    private final long[] numSubpartitionBytes;
    /** Number of bytes already read from this sort buffer for each subpartition. */
    private final long[] numSubpartitionBytesRead;
    /** Whether this sort buffer for each subpartition is finished. */
    private final int[] isChannelReadFinish;
    /** Total number of events for each sub partition. */
    private final int[] numEvents;

    // ---------------------------------------------------------------------------------------------
    // For writing
    // ---------------------------------------------------------------------------------------------
    /** Whether this sort buffer is released. A released sort buffer can not be used. */
    @GuardedBy("lock")
    private boolean isReleased;
    /** Array index in the segment list of the current available buffer for writing. */
    private int writeSegmentIndex;

    // ---------------------------------------------------------------------------------------------
    // For reading
    // ---------------------------------------------------------------------------------------------
    /** Next position in the current available buffer for writing. */
    private int writeSegmentOffset;
    /** Index entry address of the current record or event to be read. */
    private long readIndexEntryAddress;
    /** Index entry address of the current record or event to be read for each subpartition. */
    private final long[] channelReadIndexAddress;
    /** Record bytes remaining after last copy for each subpartition. */
    private final int[] channelRemainingBytes;

    /** Record bytes remaining after last copy, which must be read first in next copy. */
    private int recordRemainingBytes;

    /** Used to index the current available channel to read data from. */
    private int readOrderIndex = -1;

    public PartitionSortedBuffer(
            BufferPool bufferPool,
            int numSubpartitions,
            int bufferSize,
            @Nullable int[] customReadOrder) {
        checkArgument(bufferSize > INDEX_ENTRY_SIZE, "Buffer size is too small.");

        this.lock = new Object();
        this.bufferPool = checkNotNull(bufferPool);
        this.bufferSize = bufferSize;
        this.firstIndexEntryAddresses = new long[numSubpartitions];
        this.lastIndexEntryAddresses = new long[numSubpartitions];
        this.numSubpartitionBytes = new long[numSubpartitions];
        this.numSubpartitionBytesRead = new long[numSubpartitions];
        this.channelReadIndexAddress = new long[numSubpartitions];
        this.channelRemainingBytes = new int[numSubpartitions];
        this.isChannelReadFinish = new int[numSubpartitions];
        this.numEvents = new int[numSubpartitions];

        // initialized with -1 means the corresponding channel has no data.
        Arrays.fill(firstIndexEntryAddresses, -1L);
        Arrays.fill(lastIndexEntryAddresses, -1L);
        Arrays.fill(numSubpartitionBytes, 0L);
        Arrays.fill(numSubpartitionBytesRead, 0L);
        Arrays.fill(channelReadIndexAddress, -1L);
        Arrays.fill(channelRemainingBytes, 0);
        Arrays.fill(isChannelReadFinish, 0);
        Arrays.fill(numEvents, 0);

        this.subpartitionReadOrder = new int[numSubpartitions];
        if (customReadOrder != null) {
            checkArgument(customReadOrder.length == numSubpartitions, "Illegal data read order.");
            System.arraycopy(customReadOrder, 0, this.subpartitionReadOrder, 0, numSubpartitions);
        } else {
            for (int channel = 0; channel < numSubpartitions; ++channel) {
                this.subpartitionReadOrder[channel] = channel;
            }
        }
    }

    @Override
    public boolean append(ByteBuffer source, int targetChannel, DataType dataType)
            throws IOException {
        checkArgument(source.hasRemaining(), "Cannot append empty data.");
        checkState(!isFinished, "Sort buffer is already finished.");
        checkState(!isReleased, "Sort buffer is already released.");

        int totalBytes = source.remaining();

        // return false directly if it can not allocate enough buffers for the given record
        if (!allocateBuffersForRecord(totalBytes)) {
            return false;
        }

        // write the index entry and record or event data
        writeIndex(targetChannel, totalBytes, dataType);
        writeRecord(source);

        ++numTotalRecords;
        numTotalBytes += totalBytes;
        numSubpartitionBytes[targetChannel] += totalBytes;

        return true;
    }

    private void writeIndex(int channelIndex, int numRecordBytes, DataType dataType) {
        MemorySegment segment = buffers.get(writeSegmentIndex);

        // record length takes the high 32 bits and data type takes the low 32 bits
        segment.putLong(writeSegmentOffset, ((long) numRecordBytes << 32) | dataType.ordinal());

        // segment index takes the high 32 bits and segment offset takes the low 32 bits
        long indexEntryAddress = ((long) writeSegmentIndex << 32) | writeSegmentOffset;

        long lastIndexEntryAddress = lastIndexEntryAddresses[channelIndex];
        lastIndexEntryAddresses[channelIndex] = indexEntryAddress;

        if (lastIndexEntryAddress >= 0) {
            // link the previous index entry of the given channel to the new index entry
            segment = buffers.get(getSegmentIndexFromPointer(lastIndexEntryAddress));
            segment.putLong(
                    getSegmentOffsetFromPointer(lastIndexEntryAddress) + 8, indexEntryAddress);
        } else {
            firstIndexEntryAddresses[channelIndex] = indexEntryAddress;
        }

        // move the writer position forward to write the corresponding record
        updateWriteSegmentIndexAndOffset(INDEX_ENTRY_SIZE);
        if (!dataType.isBuffer()) {
            numEvents[channelIndex]++;
        }
    }

    private void writeRecord(ByteBuffer source) {
        while (source.hasRemaining()) {
            MemorySegment segment = buffers.get(writeSegmentIndex);
            int toCopy = Math.min(bufferSize - writeSegmentOffset, source.remaining());
            segment.put(writeSegmentOffset, source, toCopy);

            // move the writer position forward to write the remaining bytes or next record
            updateWriteSegmentIndexAndOffset(toCopy);
        }
    }

    private boolean allocateBuffersForRecord(int numRecordBytes) throws IOException {
        int numBytesRequired = INDEX_ENTRY_SIZE + numRecordBytes;
        int availableBytes =
                writeSegmentIndex == buffers.size() ? 0 : bufferSize - writeSegmentOffset;

        // return directly if current available bytes is adequate
        if (availableBytes >= numBytesRequired) {
            return true;
        }

        // skip the remaining free space if the available bytes is not enough for an index entry
        if (availableBytes < INDEX_ENTRY_SIZE) {
            updateWriteSegmentIndexAndOffset(availableBytes);
            availableBytes = 0;
        }

        // allocate exactly enough buffers for the appended record
        do {
            MemorySegment segment = requestBufferFromPool();
            if (segment == null) {
                // return false if we can not allocate enough buffers for the appended record
                return false;
            }

            availableBytes += bufferSize;
            addBuffer(segment);
        } while (availableBytes < numBytesRequired);

        return true;
    }

    private void addBuffer(MemorySegment segment) {
        synchronized (lock) {
            if (segment.size() != bufferSize) {
                bufferPool.recycle(segment);
                throw new IllegalStateException("Illegal memory segment size.");
            }

            if (isReleased) {
                bufferPool.recycle(segment);
                throw new IllegalStateException("Sort buffer is already released.");
            }

            buffers.add(segment);
        }
    }

    private MemorySegment requestBufferFromPool() throws IOException {
        try {
            // blocking request buffers if there is still guaranteed memory
            if (buffers.size() < bufferPool.getNumberOfRequiredMemorySegments()) {
                return bufferPool.requestMemorySegmentBlocking();
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while requesting buffer.");
        }

        return bufferPool.requestMemorySegment();
    }

    private void updateWriteSegmentIndexAndOffset(int numBytes) {
        writeSegmentOffset += numBytes;

        // using the next available free buffer if the current is full
        if (writeSegmentOffset == bufferSize) {
            ++writeSegmentIndex;
            writeSegmentOffset = 0;
        }
    }

    @Override
    public BufferWithChannel copyIntoSegment(
            MemorySegment target, BufferRecycler recycler, int offset) {
        synchronized (lock) {
            checkState(hasRemaining(), "No data remaining.");
            checkState(isFinished, "Should finish the sort buffer first before coping any data.");
            checkState(!isReleased, "Sort buffer is already released.");

            int numBytesCopied = 0;
            DataType bufferDataType = DataType.DATA_BUFFER;
            int channelIndex = subpartitionReadOrder[readOrderIndex];

            checkState(
                    numSubpartitionBytesRead[channelIndex] < numSubpartitionBytes[channelIndex],
                    "Bug, read too much data in sort buffer");

            do {
                int sourceSegmentIndex = getSegmentIndexFromPointer(readIndexEntryAddress);
                int sourceSegmentOffset = getSegmentOffsetFromPointer(readIndexEntryAddress);
                MemorySegment sourceSegment = buffers.get(sourceSegmentIndex);

                long lengthAndDataType = sourceSegment.getLong(sourceSegmentOffset);
                int length = getSegmentIndexFromPointer(lengthAndDataType);
                DataType dataType =
                        DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

                // return the data read directly if the next to read is an event
                if (dataType.isEvent() && numBytesCopied > 0) {
                    break;
                }
                bufferDataType = dataType;

                // get the next index entry address and move the read position forward
                long nextReadIndexEntryAddress = sourceSegment.getLong(sourceSegmentOffset + 8);
                sourceSegmentOffset += INDEX_ENTRY_SIZE;

                // throws if the event is too big to be accommodated by a buffer.
                if (bufferDataType.isEvent() && target.size() < length) {
                    throw new FlinkRuntimeException(
                            "Event is too big to be accommodated by a buffer");
                }

                numBytesCopied +=
                        copyRecordOrEvent(
                                target,
                                numBytesCopied + offset,
                                sourceSegmentIndex,
                                sourceSegmentOffset,
                                length);

                if (recordRemainingBytes == 0) {
                    // move to next channel if the current channel has been finished
                    if (readIndexEntryAddress == lastIndexEntryAddresses[channelIndex]) {
                        checkState(
                                numSubpartitionBytesRead[channelIndex] + numBytesCopied
                                        == numSubpartitionBytes[channelIndex],
                                "Read un-completely.");
                        updateReadChannelAndIndexEntryAddress();
                        break;
                    }
                    readIndexEntryAddress = nextReadIndexEntryAddress;
                }
            } while (numBytesCopied < target.size() - offset && bufferDataType.isBuffer());

            numTotalBytesRead += numBytesCopied;
            numSubpartitionBytesRead[channelIndex] += numBytesCopied;
            Buffer buffer =
                    new NetworkBuffer(target, recycler, bufferDataType, numBytesCopied + offset);
            return new BufferWithChannel(buffer, channelIndex);
        }
    }

    public int getSubpartitionReadOrderIndex(int channelIndex) {
        return subpartitionReadOrder[channelIndex];
    }

    @Nullable
    @Override
    public BufferWithChannel copyChannelBuffersIntoSegment(
            MemorySegment target, int channelIndex, BufferRecycler recycler, int offset) {
        synchronized (lock) {
            checkState(hasRemaining(), "No data remaining.");
            checkState(isFinished, "Should finish the sort buffer first before coping any data.");
            checkState(!isReleased, "Sort buffer is already released.");
            checkState(channelIndex >= 0, "Wrong channel index.");
            int targetChannelIndex = subpartitionReadOrder[channelIndex];

            int numBytesCopied = 0;
            DataType bufferDataType = DataType.DATA_BUFFER;
            if (channelReadIndexAddress[channelIndex] < 0 || hasChannelReadFinish(channelIndex)) {
                recycler.recycle(target);
                return null;
            }

            checkState(
                    numSubpartitionBytesRead[targetChannelIndex]
                            < numSubpartitionBytes[targetChannelIndex],
                    "Bug: read too much data from sort buffer.");

            do {
                int sourceSegmentIndex =
                        getSegmentIndexFromPointer(channelReadIndexAddress[channelIndex]);
                int sourceSegmentOffset =
                        getSegmentOffsetFromPointer(channelReadIndexAddress[channelIndex]);
                MemorySegment sourceSegment = buffers.get(sourceSegmentIndex);

                long lengthAndDataType = sourceSegment.getLong(sourceSegmentOffset);
                int length = getSegmentIndexFromPointer(lengthAndDataType);
                DataType dataType =
                        DataType.values()[getSegmentOffsetFromPointer(lengthAndDataType)];

                // return the data read directly if the next to read is an event
                if (dataType.isEvent() && numBytesCopied > 0) {
                    break;
                }
                bufferDataType = dataType;

                // get the next index entry address and move the read position forward
                long nextReadIndexEntryAddress = sourceSegment.getLong(sourceSegmentOffset + 8);
                sourceSegmentOffset += INDEX_ENTRY_SIZE;

                // throws if the event is too big to be accommodated by a buffer.
                if (bufferDataType.isEvent() && target.size() < length) {
                    throw new FlinkRuntimeException(
                            "Event is too big to be accommodated by a buffer");
                }

                numBytesCopied +=
                        copyChannelRecordOrEvent(
                                target,
                                numBytesCopied + offset,
                                sourceSegmentIndex,
                                sourceSegmentOffset,
                                length,
                                channelIndex);

                if (isChannelReadFinished(
                        channelIndex,
                        targetChannelIndex,
                        numBytesCopied,
                        nextReadIndexEntryAddress)) {
                    break;
                }
            } while (numBytesCopied < target.size() - offset && bufferDataType.isBuffer());

            numSubpartitionBytesRead[targetChannelIndex] += numBytesCopied;
            numTotalBytesRead += numBytesCopied;
            Buffer buffer =
                    new NetworkBuffer(target, recycler, bufferDataType, numBytesCopied + offset);
            return new BufferWithChannel(buffer, targetChannelIndex);
        }
    }

    private boolean isChannelReadFinished(
            int channelIndex,
            int targetChannelIndex,
            int numBytesCopied,
            long nextReadIndexEntryAddress) {
        if (channelRemainingBytes[channelIndex] == 0) {
            if (channelReadIndexAddress[channelIndex]
                    == lastIndexEntryAddresses[targetChannelIndex]) {
                checkState(
                        numSubpartitionBytesRead[targetChannelIndex] + numBytesCopied
                                == numSubpartitionBytes[targetChannelIndex],
                        "Read un-completely.");
                isChannelReadFinish[channelIndex] = READ_CHANNEL_FINISHED;
                return true;
            }
            channelReadIndexAddress[channelIndex] = nextReadIndexEntryAddress;
        }
        return false;
    }

    private boolean hasChannelReadFinish(int channelIndex) {
        return isChannelReadFinish[channelIndex] == READ_CHANNEL_FINISHED;
    }

    private int copyChannelRecordOrEvent(
            MemorySegment targetSegment,
            int targetSegmentOffset,
            int sourceSegmentIndex,
            int sourceSegmentOffset,
            int recordLength,
            int channelIndex) {
        if (channelRemainingBytes[channelIndex] > 0) {
            // skip the data already read if there is remaining partial record after the previous
            // copy
            long position =
                    (long) sourceSegmentOffset
                            + (recordLength - channelRemainingBytes[channelIndex]);
            sourceSegmentIndex += (position / bufferSize);
            sourceSegmentOffset = (int) (position % bufferSize);
        } else {
            channelRemainingBytes[channelIndex] = recordLength;
        }

        int targetSegmentSize = targetSegment.size();
        int numBytesToCopy =
                Math.min(
                        targetSegmentSize - targetSegmentOffset,
                        channelRemainingBytes[channelIndex]);
        do {
            // move to next data buffer if all data of the current buffer has been copied
            if (sourceSegmentOffset == bufferSize) {
                ++sourceSegmentIndex;
                sourceSegmentOffset = 0;
            }

            int numBytes =
                    copyToTargetSegment(
                            targetSegment,
                            targetSegmentOffset,
                            sourceSegmentIndex,
                            sourceSegmentOffset,
                            targetSegmentSize,
                            channelRemainingBytes[channelIndex]);

            channelRemainingBytes[channelIndex] -= numBytes;
            targetSegmentOffset += numBytes;
            sourceSegmentOffset += numBytes;
        } while (channelRemainingBytes[channelIndex] > 0
                && targetSegmentOffset < targetSegmentSize);

        return numBytesToCopy;
    }

    private int copyToTargetSegment(
            MemorySegment targetSegment,
            int targetSegmentOffset,
            int sourceSegmentIndex,
            int sourceSegmentOffset,
            int targetSegmentSize,
            int numRemainingBytes) {
        int sourceRemainingBytes = Math.min(bufferSize - sourceSegmentOffset, numRemainingBytes);
        int numBytes = Math.min(targetSegmentSize - targetSegmentOffset, sourceRemainingBytes);
        MemorySegment sourceSegment = buffers.get(sourceSegmentIndex);
        sourceSegment.copyTo(sourceSegmentOffset, targetSegment, targetSegmentOffset, numBytes);
        return numBytes;
    }

    private int copyRecordOrEvent(
            MemorySegment targetSegment,
            int targetSegmentOffset,
            int sourceSegmentIndex,
            int sourceSegmentOffset,
            int recordLength) {
        if (recordRemainingBytes > 0) {
            // skip the data already read if there is remaining partial record after the previous
            // copy
            long position = (long) sourceSegmentOffset + (recordLength - recordRemainingBytes);
            sourceSegmentIndex += (position / bufferSize);
            sourceSegmentOffset = (int) (position % bufferSize);
        } else {
            recordRemainingBytes = recordLength;
        }

        int targetSegmentSize = targetSegment.size();
        int numBytesToCopy =
                Math.min(targetSegmentSize - targetSegmentOffset, recordRemainingBytes);
        do {
            // move to next data buffer if all data of the current buffer has been copied
            if (sourceSegmentOffset == bufferSize) {
                ++sourceSegmentIndex;
                sourceSegmentOffset = 0;
            }

            int numBytes =
                    copyToTargetSegment(
                            targetSegment,
                            targetSegmentOffset,
                            sourceSegmentIndex,
                            sourceSegmentOffset,
                            targetSegmentSize,
                            recordRemainingBytes);

            recordRemainingBytes -= numBytes;
            targetSegmentOffset += numBytes;
            sourceSegmentOffset += numBytes;
        } while (recordRemainingBytes > 0 && targetSegmentOffset < targetSegmentSize);

        return numBytesToCopy;
    }

    private void updateReadChannelAndIndexEntryAddress() {
        // skip the channels without any data
        while (++readOrderIndex < firstIndexEntryAddresses.length) {
            int channelIndex = subpartitionReadOrder[readOrderIndex];
            if ((readIndexEntryAddress = firstIndexEntryAddresses[channelIndex]) >= 0) {
                break;
            }
        }
    }

    private void initChannelReadIndexEntryAddresses() {
        int channelIndex = 0;
        while (channelIndex < firstIndexEntryAddresses.length) {
            channelReadIndexAddress[channelIndex] =
                    firstIndexEntryAddresses[subpartitionReadOrder[channelIndex]];
            channelIndex++;
        }
    }

    private int getSegmentIndexFromPointer(long value) {
        return (int) (value >>> 32);
    }

    private int getSegmentOffsetFromPointer(long value) {
        return (int) (value);
    }

    @Override
    public long numRecords() {
        return numTotalRecords;
    }

    @Override
    public long numBytes() {
        return numTotalBytes;
    }

    @Override
    public long numSubpartitionBytes(int targetSubpartition) {
        return numSubpartitionBytes[subpartitionReadOrder[targetSubpartition]];
    }

    @Override
    public int numEvents(int targetSubpartition) {
        return numEvents[targetSubpartition];
    }

    @Override
    public boolean hasSubpartitionReadFinish(int targetSubpartition) {
        return numSubpartitionBytesRead[targetSubpartition]
                == numSubpartitionBytes[targetSubpartition];
    }

    @Override
    public boolean hasRemaining() {
        return numTotalBytesRead < numTotalBytes;
    }

    @Override
    public void finish() {
        checkState(
                !isFinished,
                "com.alibaba.flink.shuffle.plugin.transfer.SortBuffer is already finished.");

        isFinished = true;

        // prepare for reading
        updateReadChannelAndIndexEntryAddress();
        initChannelReadIndexEntryAddresses();
    }

    @Override
    public boolean isFinished() {
        return isFinished;
    }

    @Override
    public void release() {
        // the sort buffer can be released by other threads
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;

            for (MemorySegment segment : buffers) {
                bufferPool.recycle(segment);
            }
            buffers.clear();

            numTotalBytes = 0;
            numTotalRecords = 0;
            Arrays.fill(numSubpartitionBytes, 0L);
        }
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }
}
