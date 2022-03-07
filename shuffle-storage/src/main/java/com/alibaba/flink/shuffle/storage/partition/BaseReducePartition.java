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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutor;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.BufferListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.ReducePartition;
import com.alibaba.flink.shuffle.storage.exception.ConcurrentWriteException;
import com.alibaba.flink.shuffle.storage.utils.DataPartitionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * Base {@link ReducePartition} implementation which takes care of allocating resources and io
 * scheduling. It can be used by different subclasses and simplify the new {@link ReducePartition}
 * implementation.
 */
public abstract class BaseReducePartition extends BaseDataPartition implements ReducePartition {
    private static final Logger LOG = LoggerFactory.getLogger(BaseReducePartition.class);

    private static final Random RANDOM = new Random();

    /** Task responsible for writing data to this {@link ReducePartition}. */
    private final ReducePartitionWritingTask writingTask;

    /** Task responsible for reading data from this {@link ReducePartition}. */
    private final ReducePartitionReadingTask readingTask;

    /** Whether this {@link ReducePartition} has finished writing all data. */
    protected boolean isFinished;

    protected final BlockingQueue<DataPartitionWriter> pendingBufferWriters =
            new LinkedBlockingQueue<>();

    protected volatile boolean allocatedBuffers;

    protected int writingCounter;

    private long lastPollWriterTimestamp = System.currentTimeMillis();

    public BaseReducePartition(PartitionedDataStore dataStore, SingleThreadExecutor mainExecutor) {
        super(dataStore, mainExecutor);

        Configuration configuration = dataStore.getConfiguration();
        this.readingTask = new ReducePartitionReadingTask(configuration);
        this.writingTask = new ReducePartitionWritingTask(configuration);
        this.allocatedBuffers = false;
        this.writingCounter = 0;
    }

    @Override
    public DataPartitionWriter createPartitionWriter(
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener)
            throws Exception {
        CommonUtils.checkArgument(mapPartitionID != null, "Must be not null.");
        CommonUtils.checkArgument(dataRegionCreditListener != null, "Must be not null.");
        CommonUtils.checkArgument(failureListener != null, "Must be not null.");

        final DataPartitionWriter writer =
                getDataPartitionWriter(mapPartitionID, dataRegionCreditListener, failureListener);

        addPartitionProcessingTask(
                () -> {
                    try {
                        CommonUtils.checkArgument(
                                mapPartitionID.equals(writer.getMapPartitionID()),
                                "Inconsistent partition ID for the targetStorageTestUt map partition.");

                        final Exception exception;
                        if (isReleased) {
                            exception = new ShuffleException("Data partition has been released.");
                        } else if (!writers.isEmpty() && writers.containsKey(mapPartitionID)) {
                            exception =
                                    new ConcurrentWriteException(
                                            "Trying to write an existing map partition.");
                        } else {
                            exception = null;
                        }

                        if (exception != null) {
                            DataPartitionUtils.releaseDataPartitionWriter(writer, exception);
                            return;
                        }

                        writers.put(mapPartitionID, writer);
                    } catch (Throwable throwable) {
                        CommonUtils.runQuietly(() -> releaseOnInternalError(throwable));
                        LOG.error("Failed to create data partition writer.", throwable);
                    }
                },
                true);
        return writer;
    }

    @Override
    public DataPartitionReader createPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener)
            throws Exception {
        CommonUtils.checkArgument(dataListener != null, "Must be not null.");
        CommonUtils.checkArgument(backlogListener != null, "Must be not null.");
        CommonUtils.checkArgument(failureListener != null, "Must be not null.");

        final DataPartitionReader reader =
                getDataPartitionReader(
                        startPartitionIndex,
                        endPartitionIndex,
                        dataListener,
                        backlogListener,
                        failureListener);

        addPartitionProcessingTask(
                () -> {
                    try {
                        checkState(!isReleased, "Data partition has been released.");

                        // allocate resources when the first reader is registered
                        boolean allocateResources = readers.isEmpty();
                        readers.add(reader);

                        if (allocateResources) {
                            DataPartitionReadingTask readingTask =
                                    CommonUtils.checkNotNull(getPartitionReadingTask());
                            readingTask.allocateResources();
                        }
                    } catch (Throwable throwable) {
                        DataPartitionUtils.releaseDataPartitionReader(reader, throwable);
                        LOG.error("Failed to create data partition reader.", throwable);
                    }
                },
                true);
        return reader;
    }

    /**
     * Returns the corresponding {@link DataPartitionReader} of the target reduce partitions. The
     * implementation is responsible for closing its allocated resources if any when encountering
     * any exception.
     */
    protected abstract DataPartitionReader getDataPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener)
            throws Exception;

    /**
     * Returns the corresponding {@link DataPartitionWriter} of the target reduce partition. The
     * implementation is responsible for closing its allocated resources if any when encountering
     * any exception.
     */
    protected abstract DataPartitionWriter getDataPartitionWriter(
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener)
            throws Exception;

    @Override
    public ReducePartitionWritingTask getPartitionWritingTask() {
        return writingTask;
    }

    @Override
    protected int numWritingCounter() {
        return writingCounter;
    }

    @Override
    protected int numWritingTaskBuffers() {
        return writingTask.buffers.size();
    }

    @Override
    public ReducePartitionReadingTask getPartitionReadingTask() {
        return readingTask;
    }

    @Override
    protected void decWritingCount() {
        writingCounter--;
    }

    @Override
    protected void releaseInternal(Throwable releaseCause) throws Exception {
        Throwable exception = null;

        try {
            super.releaseInternal(releaseCause);
        } catch (Throwable throwable) {
            exception = throwable;
            LOG.error("Fatal: failed to release base data partition.", throwable);
        }

        try {
            writingTask.release(this.releaseCause);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Fatal: failed to release data writing task.", throwable);
        }

        try {
            readingTask.release(this.releaseCause);
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Fatal: failed to release data reading task.", throwable);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    @Override
    public void addPendingBufferWriter(DataPartitionWriter writer) {
        pendingBufferWriters.add(writer);
    }

    public BlockingQueue<DataPartitionWriter> getPendingBufferWriters() {
        return pendingBufferWriters;
    }

    /**
     * {@link ReducePartitionWritingTask} implements the basic resource allocation and data
     * processing logic which can be reused by subclasses.
     */
    protected class ReducePartitionWritingTask implements DataPartitionWritingTask, BufferListener {

        /**
         * Minimum size of memory in bytes to trigger writing of {@link DataPartitionReader}s. Use a
         * portion of the guaranteed memory for data bulk writing and keep the left memory for data
         * transmission over the network, better writing pipeline can be achieved (1/2 is just an
         * empirical value).
         */
        public final int minMemoryToWrite =
                CommonUtils.checkedDownCast(
                        StorageOptions.MIN_WRITING_READING_MEMORY_SIZE.divide(2).getBytes());

        /**
         * Minimum number of buffers (calculated from {@link #minMemoryToWrite} and buffer size) to
         * trigger writing of {@link DataPartitionWriter}s.
         */
        public final int minBuffersToWrite;

        /**
         * Minimum number of buffers (calculated from buffer size and {@link
         * StorageOptions#MIN_WRITING_READING_MEMORY_SIZE}) to be used for data partition writing.
         */
        protected final int minWritingBuffers;

        /**
         * Maximum number of buffers (calculated from buffer size and the configured value for
         * {@link StorageOptions#STORAGE_MAX_PARTITION_WRITING_MEMORY}) to be used for data
         * partition writing.
         */
        protected final int maxWritingBuffers;

        /** Available buffers can be used for data writing of the target partition. */
        protected final BufferQueue buffers = new BufferQueue(new ArrayList<>());

        private int numTotalBuffers;

        protected ReducePartitionWritingTask(Configuration configuration) {
            int minWritingMemory =
                    CommonUtils.checkedDownCast(
                            StorageOptions.MIN_WRITING_READING_MEMORY_SIZE.getBytes());
            int maxWritingMemory =
                    CommonUtils.checkedDownCast(
                            configuration
                                    .getMemorySize(
                                            StorageOptions.STORAGE_MAX_PARTITION_WRITING_MEMORY)
                                    .getBytes());
            int bufferSize =
                    CommonUtils.checkedDownCast(
                            configuration
                                    .getMemorySize(MemoryOptions.MEMORY_BUFFER_SIZE)
                                    .getBytes());
            this.minBuffersToWrite = Math.max(1, minMemoryToWrite / bufferSize);
            this.minWritingBuffers = Math.max(1, minWritingMemory / bufferSize);
            this.maxWritingBuffers = Math.max(minWritingBuffers, maxWritingMemory / bufferSize);
        }

        @Override
        public void process() {
            try {
                CommonUtils.checkState(inExecutorThread(), "Not in main thread.");

                if (isReleased || writers.isEmpty()) {
                    return;
                }
                CommonUtils.checkState(!buffers.isReleased(), "Buffers has been released.");

                Throwable throwable = null;
                for (DataPartitionWriter writer : writers.values()) {
                    try {
                        if (writer.writeData()) {
                            LOG.debug(
                                    "Successfully write partition data: {}, {}.",
                                    writer.getMapPartitionID(),
                                    getPartitionMeta());
                        }
                    } catch (Throwable th) {
                        throwable = th;
                        LOG.error("Write data failed, ", th);
                        break;
                    }
                }

                if (isReleased || isFinished) {
                    recycleResources();
                    return;
                }

                dispatchBuffers();

                if (throwable != null) {
                    DataPartitionUtils.releaseDataPartitionWriters(writers.values(), throwable);
                    recycleBuffers(buffers.release(), dataStore.getWritingBufferDispatcher());
                    LOG.error("Failed to write partition data.", throwable);
                }
            } catch (Throwable throwable) {
                DataPartitionUtils.releaseDataPartitionWriters(writers.values(), throwable);
                recycleBuffers(buffers.release(), dataStore.getWritingBufferDispatcher());
                LOG.error("Failed to write partition data.", throwable);
                CommonUtils.runQuietly(() -> releaseOnInternalError(throwable));
            }
        }

        private void checkInProcessState() {
            checkState(writers.size() > 0, "No registered writer.");
            checkState(!isReleased, "Partition has been released.");
            checkState(!isFinished, "Data writing has finished.");
        }

        @Override
        public void allocateResources() {
            checkState(inExecutorThread(), "Not in main thread.");
            checkInProcessState();

            if (!allocatedBuffers) {
                allocateBuffers(
                        dataStore.getWritingBufferDispatcher(),
                        this,
                        minWritingBuffers,
                        maxWritingBuffers);
                allocatedBuffers = true;
            }
        }

        @Override
        public void release(@Nullable Throwable releaseCause) throws Exception {
            try {
                checkState(inExecutorThread(), "Not in main thread.");

                DataPartitionUtils.releaseDataPartitionWriters(writers.values(), releaseCause);
                recycleBuffers(buffers.release(), dataStore.getWritingBufferDispatcher());
                numTotalBuffers = 0;
            } catch (Throwable throwable) {
                LOG.error("Fatal: failed to release the data writing task.", throwable);
                ExceptionUtils.rethrowException(throwable);
            }
        }

        @Override
        public void triggerWriting() {
            addPartitionProcessingTask(this);
        }

        @Override
        public void recycleResources() {
            List<ByteBuffer> toRelease = new ArrayList<>(buffers.size());
            while (buffers.size() > 0) {
                toRelease.add(buffers.poll());
            }
            recycleBuffers(toRelease, dataStore.getWritingBufferDispatcher());
            allocatedBuffers = false;
            numTotalBuffers = 0;
            checkState(pendingBufferWriters.isEmpty(), "Non-empty pending buffer writers.");
            checkState(buffers.size() == 0, "Bug: leaking buffers.");
        }

        @Override
        public void finishInput() throws Exception {
            checkState(inExecutorThread(), "Not in main thread.");
            checkInProcessState();
            isFinished = true;
            for (DataPartitionWriter writer : writers.values()) {
                writer.finishPartitionInput();
            }
        }

        private void dispatchBuffers() {
            checkState(inExecutorThread(), "Not in main thread.");
            checkInProcessState();

            while (!pendingBufferWriters.isEmpty()) {
                DataPartitionWriter writer = pendingBufferWriters.peek();
                if (writer.isCreditFulfilled() || writer.isRegionFinished()) {
                    randomlyPollNextWriter();
                    continue;
                }
                boolean isWritingCounterIncreased = false;
                if (writer.numFulfilledCredit() == 0 && writer.numPendingCredit() != 0) {
                    isWritingCounterIncreased = true;
                    writingCounter++;
                }

                if (!assignBuffersToWriters(writer, isWritingCounterIncreased)) {
                    break;
                }
            }
            recycleBuffersIfNeeded();
        }

        private void randomlyPollNextWriter() {
            if (pendingBufferWriters.isEmpty()) {
                return;
            }
            int numFulfilled = pendingBufferWriters.peek().numFulfilledCredit();
            pendingBufferWriters.poll();
            if (pendingBufferWriters.size() > 1) {
                int numMove = RANDOM.nextInt(pendingBufferWriters.size());
                while (numMove-- > 0) {
                    pendingBufferWriters.add(checkNotNull(pendingBufferWriters.poll()));
                }
            }
            LOG.info(
                    "{}, fulfilled {} credits for {} ms, next need {} credits, total buffers {}",
                    this,
                    numFulfilled,
                    (System.currentTimeMillis() - lastPollWriterTimestamp),
                    pendingBufferWriters.peek() == null
                            ? 0
                            : pendingBufferWriters.peek().numPendingCredit(),
                    buffers.size());
            lastPollWriterTimestamp = System.currentTimeMillis();
        }

        /**
         * Returning true indicates that the available buffer is sufficient to meet the buffer
         * request of the writer in the current queue header.
         */
        private boolean assignBuffersToWriters(
                DataPartitionWriter writer, boolean isWritingCounterIncreased) {
            int requireCredit = writer.numPendingCredit();
            if (buffers.size() < requireCredit) {
                //                if (buffers.size() < MIN_CREDITS_TO_NOTIFY
                //                        && requireCredit >= MIN_CREDITS_TO_NOTIFY) {
                //                    return false;
                //                }

                //                if (numTotalBuffers == buffers.size() && writingCounter == 1) {
                //                    assignBuffers(writer, buffers, false);
                //                    checkState(!writer.isCreditFulfilled(), "Wrong credit state");
                //                }
                assignBuffers(writer, buffers, false);
                checkState(!writer.isCreditFulfilled(), "Wrong credit state");
                return false;
            }

            fulfillCurrentWriterCreditRequirement(writer, isWritingCounterIncreased, requireCredit);
            return true;
        }

        private void fulfillCurrentWriterCreditRequirement(
                DataPartitionWriter writer, boolean isWritingCounterIncreased, int requireCredit) {
            BufferQueue assignBuffers = new BufferQueue(new ArrayList<>());
            IntStream.range(0, requireCredit).forEach(i -> assignBuffers.add(buffers.poll()));
            int numAssigned = assignBuffers(writer, assignBuffers, false);
            if (numAssigned == requireCredit) {
                randomlyPollNextWriter();
                checkState(
                        writer.isCreditFulfilled() || writer.isRegionFinished(),
                        "Wrong credit state");
            } else {
                checkState(!writer.isCreditFulfilled(), "Wrong credit state");
                writingCounter = isWritingCounterIncreased ? writingCounter - 1 : writingCounter;
            }
        }

        private void recycleBuffersIfNeeded() {
            if (pendingBufferWriters.isEmpty() && buffers.size() == numTotalBuffers) {
                recycleResources();
            }
        }

        private int assignBuffers(
                DataPartitionWriter writer, BufferQueue assignBuffers, boolean checkMinBuffers) {
            int numToAssignBuffers = assignBuffers.size();
            writer.assignCredits(
                    assignBuffers, buffer -> recycle(buffer, buffers), checkMinBuffers);
            int numAssigned = numToAssignBuffers - assignBuffers.size();
            while (assignBuffers.size() > 0) {
                buffers.add(assignBuffers.poll());
            }
            return numAssigned;
        }

        /** Notifies the allocated writing buffers to this data writing task. */
        @Override
        public void notifyBuffers(List<ByteBuffer> allocatedBuffers, Throwable exception) {
            addPartitionProcessingTask(
                    () -> {
                        try {
                            if (exception != null) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getWritingBufferDispatcher());
                                throw exception;
                            }

                            CommonUtils.checkArgument(
                                    allocatedBuffers != null && !allocatedBuffers.isEmpty(),
                                    "Fatal: empty buffer was allocated.");

                            if (isReleased || isFinished) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getWritingBufferDispatcher());
                                return;
                            }

                            if (buffers.isReleased()) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getWritingBufferDispatcher());
                                throw new ShuffleException("Buffers has been released.");
                            }

                            numTotalBuffers += allocatedBuffers.size();
                            buffers.add(allocatedBuffers);
                            allocatedBuffers.clear();
                            dispatchBuffers();
                        } catch (Throwable throwable) {
                            CommonUtils.runQuietly(() -> releaseOnInternalError(throwable));
                            LOG.error("Fatal: resource allocation error.", throwable);
                        }
                    });
        }

        private void handleRecycledBuffer(ByteBuffer buffer, BufferQueue buffers) {
            try {
                CommonUtils.checkArgument(buffer != null, "Must be not null.");

                if (isReleased || isFinished) {
                    recycleBuffers(
                            Collections.singletonList(buffer),
                            dataStore.getWritingBufferDispatcher());
                    numTotalBuffers--;
                    return;
                }

                if (buffers.isReleased()) {
                    recycleBuffers(
                            Collections.singletonList(buffer),
                            dataStore.getWritingBufferDispatcher());
                    numTotalBuffers--;
                    throw new ShuffleException("Buffers has been released.");
                }

                buffers.add(buffer);
                dispatchBuffers();
            } catch (Throwable throwable) {
                CommonUtils.runQuietly(() -> releaseOnInternalError(throwable));
                LOG.error("Resource recycling error.", throwable);
            }
        }

        /**
         * Recycles a writing buffer to this data writing task. If no more buffer is needed, the
         * recycled buffer will be returned to the buffer manager directly and if any unexpected
         * exception occurs, the corresponding data partition will be released.
         */
        public void recycle(ByteBuffer buffer, BufferQueue buffers) {
            if (!inExecutorThread()) {
                addPartitionProcessingTask(() -> handleRecycledBuffer(buffer, buffers));
                return;
            }

            handleRecycledBuffer(buffer, buffers);
        }
    }

    /**
     * {@link ReducePartitionReadingTask} implements the basic resource allocation and data reading
     * logic (including IO scheduling) which can be reused by subclasses.
     */
    protected class ReducePartitionReadingTask implements DataPartitionReadingTask, BufferListener {

        /**
         * Minimum size of memory in bytes to trigger reading of {@link DataPartitionReader}s. Use a
         * portion of the guaranteed memory for data bulk reading and keep the left memory as data
         * cache in the reading view, better reading pipeline can be achieved (1/2 is just an
         * empirical value).
         */
        public final int minMemoryToRead =
                CommonUtils.checkedDownCast(
                        StorageOptions.MIN_WRITING_READING_MEMORY_SIZE.divide(2).getBytes());

        /**
         * Minimum number of buffers (calculated from {@link #minMemoryToRead} and buffer size) to
         * trigger reading of {@link DataPartitionReader}s.
         */
        public final int minBuffersToRead;

        /**
         * Minimum number of buffers (calculated from buffer size and {@link
         * StorageOptions#MIN_WRITING_READING_MEMORY_SIZE}) to be used for data partition reading.
         */
        protected final int minReadingBuffers;

        /**
         * Maximum number of buffers (calculated from buffer size and the configured value for
         * {@link StorageOptions#STORAGE_MAX_PARTITION_READING_MEMORY}) to be used for data
         * partition reading.
         */
        protected final int maxReadingBuffers;

        /** All available buffers can be used by the partition readers for reading. */
        protected BufferQueue buffers = BufferQueue.RELEASED_EMPTY_BUFFER_QUEUE;

        protected ReducePartitionReadingTask(Configuration configuration) {
            int minReadingMemory =
                    CommonUtils.checkedDownCast(
                            StorageOptions.MIN_WRITING_READING_MEMORY_SIZE.getBytes());
            int maxReadingMemory =
                    CommonUtils.checkedDownCast(
                            configuration
                                    .getMemorySize(
                                            StorageOptions.STORAGE_MAX_PARTITION_READING_MEMORY)
                                    .getBytes());
            int bufferSize =
                    CommonUtils.checkedDownCast(
                            configuration
                                    .getMemorySize(MemoryOptions.MEMORY_BUFFER_SIZE)
                                    .getBytes());
            this.minBuffersToRead = Math.max(1, minMemoryToRead / bufferSize);
            this.minReadingBuffers = Math.max(1, minReadingMemory / bufferSize);
            this.maxReadingBuffers = Math.max(minReadingBuffers, maxReadingMemory / bufferSize);
        }

        @Override
        public void process() {
            try {
                checkState(inExecutorThread(), "Not in main thread.");

                if (isReleased) {
                    return;
                }
                checkState(!readers.isEmpty(), "No reader registered.");
                checkState(!buffers.isReleased(), "Buffers has been released.");
                BufferRecycler recycler = (buffer) -> recycle(buffer, buffers);

                for (DataPartitionReader reader : readers) {
                    if (!reader.isOpened()) {
                        reader.open();
                    }
                }
                PriorityQueue<DataPartitionReader> sortedReaders = new PriorityQueue<>(readers);

                while (buffers.size() > 0 && !sortedReaders.isEmpty()) {
                    DataPartitionReader reader = sortedReaders.poll();
                    try {
                        if (!reader.readData(buffers, recycler)) {
                            removePartitionReader(reader);
                            LOG.debug("Successfully read partition data: {}.", reader);
                        }
                    } catch (Throwable throwable) {
                        removePartitionReader(reader);
                        DataPartitionUtils.releaseDataPartitionReader(reader, throwable);
                        LOG.debug("Failed to read partition data: {}.", reader, throwable);
                    }
                }
            } catch (Throwable throwable) {
                DataPartitionUtils.releaseDataPartitionReaders(readers, throwable);
                recycleBuffers(buffers.release(), dataStore.getReadingBufferDispatcher());
                LOG.error("Fatal: failed to read partition data.", throwable);
            }
        }

        private void removePartitionReader(DataPartitionReader reader) {
            readers.remove(reader);
            if (readers.isEmpty()) {
                recycleBuffers(buffers.release(), dataStore.getReadingBufferDispatcher());
            }
        }

        /**
         * Recycles a reading buffer to this data reading task. If no more buffer is needed, the
         * recycled buffer will be returned to the buffer manager directly and if any unexpected
         * exception occurs, all registered readers will be released.
         */
        private void recycle(ByteBuffer buffer, BufferQueue bufferQueue) {
            addPartitionProcessingTask(
                    () -> {
                        try {
                            CommonUtils.checkArgument(buffer != null, "Must be not null.");

                            if (bufferQueue == null || bufferQueue.isReleased()) {
                                recycleBuffers(
                                        Collections.singletonList(buffer),
                                        dataStore.getReadingBufferDispatcher());
                                checkState(bufferQueue != null, "Must be not null.");
                                return;
                            }

                            bufferQueue.add(buffer);
                            if (bufferQueue.size() >= minBuffersToRead) {
                                triggerReading();
                            }
                        } catch (Throwable throwable) {
                            DataPartitionUtils.releaseDataPartitionReaders(readers, throwable);
                            recycleBuffers(
                                    buffers.release(), dataStore.getReadingBufferDispatcher());
                            LOG.error("Resource recycling error.", throwable);
                        }
                    });
        }

        @Override
        public void allocateResources() {
            checkState(inExecutorThread(), "Not in main thread.");
            checkState(!readers.isEmpty(), "No reader registered.");
            checkState(!isReleased, "Partition has been released.");

            allocateBuffers(
                    dataStore.getReadingBufferDispatcher(),
                    this,
                    minReadingBuffers,
                    maxReadingBuffers);
        }

        @Override
        public void triggerReading() {
            if (inExecutorThread()) {
                process();
                return;
            }

            addPartitionProcessingTask(this);
        }

        /** Notifies the allocated reading buffers to this data reading task. */
        @Override
        public void notifyBuffers(List<ByteBuffer> allocatedBuffers, Throwable exception) {
            addPartitionProcessingTask(
                    () -> {
                        try {
                            if (exception != null) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getReadingBufferDispatcher());
                                throw exception;
                            }

                            CommonUtils.checkArgument(
                                    allocatedBuffers != null && !allocatedBuffers.isEmpty(),
                                    "Fatal: empty buffer was allocated.");

                            if (!buffers.isReleased()) {
                                recycleBuffers(
                                        buffers.release(), dataStore.getReadingBufferDispatcher());
                                LOG.error("Fatal: the allocated data reading buffers are leaking.");
                            }

                            if (isReleased || readers.isEmpty()) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getReadingBufferDispatcher());
                                return;
                            }

                            buffers = new BufferQueue(allocatedBuffers);
                            triggerReading();
                        } catch (Throwable throwable) {
                            DataPartitionUtils.releaseDataPartitionReaders(readers, throwable);
                            recycleBuffers(
                                    buffers.release(), dataStore.getReadingBufferDispatcher());
                            LOG.error("Resource allocation error.", throwable);
                        }
                    });
        }

        /**
         * Releases all reading buffers and partition readers if the corresponding data partition
         * has been released.
         */
        @Override
        public void release(Throwable releaseCause) throws Exception {
            try {
                checkState(inExecutorThread(), "Not in main thread.");

                LOG.debug("Release reading buffers, buffers num={}", buffers.size());
                recycleBuffers(buffers.release(), dataStore.getReadingBufferDispatcher());
            } catch (Throwable throwable) {
                LOG.error("Fatal: failed to release the data reading task.", throwable);
                ExceptionUtils.rethrowException(throwable);
            }
        }
    }
}
