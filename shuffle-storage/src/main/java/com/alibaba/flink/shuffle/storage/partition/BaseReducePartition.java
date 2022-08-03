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
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * Base {@link ReducePartition} implementation which takes care of allocating resources and io
 * scheduling. It can be used by different subclasses and simplify the new {@link ReducePartition}
 * implementation.
 */
public abstract class BaseReducePartition extends BaseDataPartition implements ReducePartition {

    private static final Logger LOG = LoggerFactory.getLogger(BaseReducePartition.class);

    /** Task responsible for writing data to this {@link ReducePartition}. */
    private final ReducePartitionWritingTask writingTask;

    /** Task responsible for reading data from this {@link ReducePartition}. */
    private final ReducePartitionReadingTask readingTask;

    /** Whether this {@link ReducePartition} has finished writing all data. */
    protected boolean isFinished;

    /**
     * A queue that stores {@link DataPartitionWriter}s whose required buffers(credits) have not
     * been fulfilled.
     */
    protected final BlockingQueue<DataPartitionWriter> pendingBufferWriters =
            new LinkedBlockingQueue<>();

    /**
     * A queue that stores {@link DataPartitionWriter}s whose {@link BufferOrMarker}s need to be
     * processed.
     */
    protected final BlockingQueue<DataPartitionWriter> pendingProcessWriters =
            new LinkedBlockingQueue<>();

    public BaseReducePartition(PartitionedDataStore dataStore, SingleThreadExecutor mainExecutor) {
        super(dataStore, mainExecutor);

        Configuration configuration = dataStore.getConfiguration();
        this.readingTask = new ReducePartitionReadingTask(configuration);
        this.writingTask = new ReducePartitionWritingTask(configuration);
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
    protected void finishInput() {
        isFinished = true;
    }

    @Override
    public ReducePartitionReadingTask getPartitionReadingTask() {
        return readingTask;
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

    @Override
    protected void removePendingBufferWriter(DataPartitionWriter writer) {
        if (pendingBufferWriters.peek() == writer) {
            pendingBufferWriters.poll();
        }
    }

    @Override
    protected int numPendingBufferWriters() {
        return pendingBufferWriters.size();
    }

    @Override
    protected int numPendingProcessWriters() {
        return pendingProcessWriters.size();
    }

    /**
     * {@link ReducePartitionWritingTask} implements the basic resource allocation and data
     * processing logic which can be reused by subclasses.
     */
    protected class ReducePartitionWritingTask implements DataPartitionWritingTask, BufferListener {

        /**
         * Minimum size of memory in bytes to trigger writing of {@link DataPartitionWriter}s. Use a
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

        /**
         * When writing data partition, some buffers are reserved when recycling to avoid requesting
         * buffers too frequently. This is the memory size to reserve.
         */
        private final int numLazyRecycleBuffers;

        /** Available buffers can be used for data writing of the target partition. */
        protected final BufferQueue buffers =
                new BufferQueue(BaseReducePartition.this, dataStore.getWritingBufferDispatcher());

        /**
         * Whether this data writing task has allocated resources and is waiting to be fulfilled.
         */
        protected boolean isWaitingResources;

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
            int lazyRecycleBufferMemory =
                    Math.min(
                            maxWritingMemory,
                            CommonUtils.checkedDownCast(
                                    configuration
                                            .getMemorySize(
                                                    StorageOptions
                                                            .STORAGE_PARTITION_LAZY_RECYCLE_WRITING_MEMORY)
                                            .getBytes()));
            this.minBuffersToWrite = Math.max(1, minMemoryToWrite / bufferSize);
            this.minWritingBuffers = Math.max(1, minWritingMemory / bufferSize);
            this.maxWritingBuffers = Math.max(minWritingBuffers, maxWritingMemory / bufferSize);
            this.numLazyRecycleBuffers = Math.max(0, lazyRecycleBufferMemory / bufferSize);
        }

        @Override
        public void process() {
            try {
                CommonUtils.checkState(inExecutorThread(), "Not in main thread.");

                if (isReleased || pendingProcessWriters.isEmpty()) {
                    return;
                }
                CommonUtils.checkState(!buffers.isReleased(), "Buffers has been released.");

                while (!pendingProcessWriters.isEmpty()) {
                    DataPartitionWriter writer = pendingProcessWriters.peek();
                    if (writer.writeData()) {
                        LOG.debug(
                                "Successfully write partition data: {}, {}.",
                                writer.getMapPartitionID(),
                                getPartitionMeta());
                    }
                    if (writer.isWritingPartial()) {
                        break;
                    }
                    pendingProcessWriters.poll();
                    writer.setInProcessQueue(false);

                    if (pendingProcessWriters.isEmpty()) {
                        writer.triggerFlushFileDataBuffers();
                    }
                }

                if (isReleased || isFinished) {
                    recycleResources();
                    return;
                }

                dispatchBuffers();
            } catch (Throwable throwable) {
                DataPartitionUtils.releaseDataPartitionWriters(writers.values(), throwable);
                buffers.release();
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

            int numAllocate =
                    Math.min(minWritingBuffers, maxWritingBuffers - buffers.numBuffersOccupied());
            if (shouldAllocateResource(numAllocate)) {
                allocateBuffers(
                        dataStore.getWritingBufferDispatcher(), this, numAllocate, numAllocate);
                isWaitingResources = true;
            }
        }

        private boolean shouldAllocateResource(int numAllocate) {
            return numAllocate > 0
                    && !isWaitingResources
                    && !pendingBufferWriters.isEmpty()
                    && buffers.size() < minBuffersToWrite
                    && buffers.numBuffersOccupied() + numAllocate <= maxWritingBuffers;
        }

        @Override
        public void release(@Nullable Throwable releaseCause) throws Exception {
            try {
                checkState(inExecutorThread(), "Not in main thread.");

                DataPartitionUtils.releaseDataPartitionWriters(writers.values(), releaseCause);
                buffers.release();
            } catch (Throwable throwable) {
                LOG.error("Fatal: failed to release the data writing task.", throwable);
                ExceptionUtils.rethrowException(throwable);
            }
        }

        @Override
        public void triggerWriting(DataPartitionWriter writer) {
            addPartitionProcessingTask(
                    () -> {
                        if (!writer.isInProcessQueue()) {
                            pendingProcessWriters.add(writer);
                            writer.setInProcessQueue(true);
                        }
                        this.process();
                    });
        }

        @Override
        public int numAvailableBuffers() {
            return buffers.size();
        }

        @Override
        public int numOccupiedBuffers() {
            return buffers.numBuffersOccupied();
        }

        @Override
        public boolean hasAllocatedMaxWritingBuffers() {
            return buffers.numBuffersOccupied() == maxWritingBuffers;
        }

        @Override
        public void recycleResources() {
            buffers.recycleAll();
            checkState(pendingBufferWriters.isEmpty(), "Non-empty pending buffer writers.");
            checkState(buffers.size() == 0, "Bug: leaking buffers.");
        }

        private void recycleResourcesWithReserve() {
            while (buffers.size() > numLazyRecycleBuffers) {
                buffers.recycle(buffers.poll());
            }
        }

        private void dispatchBuffers() {
            checkState(inExecutorThread(), "Not in main thread.");

            if (buffers.size() == 0) {
                return;
            }

            while (!pendingBufferWriters.isEmpty() && buffers.size() > 0) {
                DataPartitionWriter writer = checkNotNull(pendingBufferWriters.peek());
                if (!writer.assignCredits(buffers, this::recycle)) {
                    pendingBufferWriters.poll();
                }
            }

            if (pendingBufferWriters.isEmpty()) {
                if (pendingProcessWriters.isEmpty()) {
                    recycleResources();
                } else {
                    recycleResourcesWithReserve();
                }
            } else {
                allocateResources();
            }
        }

        /** Notifies the allocated writing buffers to this data writing task. */
        @Override
        public void notifyBuffers(List<ByteBuffer> allocatedBuffers, Throwable exception) {
            addPartitionProcessingTask(
                    () -> {
                        try {
                            isWaitingResources = false;
                            if (exception != null) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getWritingBufferDispatcher());
                                throw exception;
                            }

                            CommonUtils.checkArgument(
                                    allocatedBuffers != null && !allocatedBuffers.isEmpty(),
                                    "Fatal: empty buffer was allocated.");

                            if (isReleased || isFinished || buffers.isReleased()) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getWritingBufferDispatcher());
                                return;
                            }

                            buffers.add(allocatedBuffers);
                            allocatedBuffers.clear();
                            dispatchBuffers();
                        } catch (Throwable throwable) {
                            CommonUtils.runQuietly(() -> releaseOnInternalError(throwable));
                            LOG.error("Fatal: resource allocation error.", throwable);
                        }
                    });
        }

        private void handleRecycledBuffer(ByteBuffer buffer) {
            try {
                CommonUtils.checkArgument(buffer != null, "Must be not null.");

                buffers.recycle(buffer);
                if (isReleased || isFinished || buffers.isReleased()) {
                    return;
                }

                allocateResources();
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
        public void recycle(ByteBuffer buffer) {
            if (!inExecutorThread()) {
                addPartitionProcessingTask(() -> handleRecycledBuffer(buffer));
                return;
            }

            handleRecycledBuffer(buffer);
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
        protected final BufferQueue buffers =
                new BufferQueue(BaseReducePartition.this, dataStore.getReadingBufferDispatcher());

        /**
         * Whether this data reading task has allocated resources and is waiting to be fulfilled.
         */
        protected boolean isWaitingResources;

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
                CommonUtils.checkState(inExecutorThread(), "Not in main thread.");

                if (isReleased) {
                    return;
                }
                CommonUtils.checkState(!readers.isEmpty(), "No reader registered.");
                CommonUtils.checkState(!buffers.isReleased(), "Buffers has been released.");

                for (DataPartitionReader reader : readers) {
                    if (!reader.isOpened()) {
                        reader.open();
                    }
                }
                PriorityQueue<DataPartitionReader> sortedReaders = new PriorityQueue<>(readers);

                while (buffers.size() > 0 && !sortedReaders.isEmpty()) {
                    DataPartitionReader reader = sortedReaders.poll();
                    try {
                        if (!reader.readData(buffers, this::recycle)) {
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
                buffers.recycleAll();
                LOG.error("Fatal: failed to read partition data.", throwable);
            }
        }

        private void removePartitionReader(DataPartitionReader reader) {
            readers.remove(reader);
            if (readers.isEmpty()) {
                buffers.recycleAll();
            }
        }

        /**
         * Recycles a reading buffer to this data reading task. If no more buffer is needed, the
         * recycled buffer will be returned to the buffer manager directly and if any unexpected
         * exception occurs, all registered readers will be released.
         */
        private void recycle(ByteBuffer buffer) {
            addPartitionProcessingTask(
                    () -> {
                        try {
                            CommonUtils.checkArgument(buffer != null, "Must be not null.");

                            buffers.recycle(buffer);
                            if (isReleased || readers.isEmpty() || buffers.isReleased()) {
                                return;
                            }

                            if (buffers.size() > 0) {
                                triggerReading();
                            }

                            if (buffers.size() < minBuffersToRead
                                    && !readers.isEmpty()
                                    && buffers.numBuffersOccupied() + minReadingBuffers
                                            <= maxReadingBuffers) {
                                allocateResources();
                            }
                        } catch (Throwable throwable) {
                            DataPartitionUtils.releaseDataPartitionReaders(readers, throwable);
                            buffers.recycleAll();
                            LOG.error("Resource recycling error.", throwable);
                        }
                    });
        }

        @Override
        public void allocateResources() {
            checkState(inExecutorThread(), "Not in main thread.");
            checkState(!readers.isEmpty(), "No reader registered.");
            checkState(!isReleased, "Partition has been released.");

            if (!isWaitingResources) {
                allocateBuffers(
                        dataStore.getReadingBufferDispatcher(),
                        this,
                        minReadingBuffers,
                        minReadingBuffers);
                isWaitingResources = true;
            }
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
                            isWaitingResources = false;
                            if (exception != null) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getReadingBufferDispatcher());
                                throw exception;
                            }

                            CommonUtils.checkArgument(
                                    allocatedBuffers != null && !allocatedBuffers.isEmpty(),
                                    "Fatal: empty buffer was allocated.");

                            if (isReleased || readers.isEmpty() || buffers.isReleased()) {
                                recycleBuffers(
                                        allocatedBuffers, dataStore.getReadingBufferDispatcher());
                                return;
                            }

                            buffers.add(allocatedBuffers);
                            triggerReading();
                        } catch (Throwable throwable) {
                            DataPartitionUtils.releaseDataPartitionReaders(readers, throwable);
                            buffers.release();
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

                buffers.release();
            } catch (Throwable throwable) {
                LOG.error("Fatal: failed to release the data reading task.", throwable);
                ExceptionUtils.rethrowException(throwable);
            }
        }
    }
}
