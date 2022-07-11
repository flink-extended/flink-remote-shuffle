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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutor;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.BufferListener;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * {@link BaseDataPartition} implements some basic logics of {@link DataPartition} which can be
 * reused by subclasses and simplify the implementation of new data partitions. It adopts a single
 * thread execution mode and all processing logics of a {@link DataPartition} will run in the main
 * executor thread. As a result, no lock is needed and the data partition processing logics can be
 * simplified.
 */
public abstract class BaseDataPartition implements DataPartition {

    private static final Logger LOG = LoggerFactory.getLogger(BaseDataPartition.class);

    /**
     * A single thread executor to process data partition events including data writing and reading.
     */
    private final SingleThreadExecutor mainExecutor;

    /** All pending {@link PartitionProcessingTask}s to be processed by the {@link #processor}. */
    private final LinkedList<PartitionProcessingTask> taskQueue = new LinkedList<>();

    /** A {@link Runnable} task responsible for processing all {@link PartitionProcessingTask}s. */
    private final DataPartitionProcessor processor = new DataPartitionProcessor();

    /** {@link PartitionedDataStore} storing this data partition. */
    protected final PartitionedDataStore dataStore;

    /** All {@link DataPartitionReader} reading this data partition. */
    protected final Set<DataPartitionReader> readers = new HashSet<>();

    /** All {@link DataPartitionWriter} writing this data partition. */
    protected final Map<MapPartitionID, DataPartitionWriter> writers = new HashMap<>();

    /** The number of {@link DataPartitionWriter}s who have processed input finish marker. */
    protected int numInputFinishWriter;

    /** The number of {@link DataPartitionWriter}s who are writing data. */
    protected int writingCounter;

    /** Whether this data partition is released or not. */
    protected boolean isReleased;

    /** The reason why this data partition is released. */
    protected Throwable releaseCause;

    public BaseDataPartition(PartitionedDataStore dataStore, SingleThreadExecutor mainExecutor) {
        CommonUtils.checkArgument(dataStore != null, "Must be not null.");
        CommonUtils.checkArgument(mainExecutor != null, "Must be not null.");

        this.dataStore = dataStore;
        this.mainExecutor = mainExecutor;
    }

    /** Returns true if the program is running in the main executor thread. */
    protected boolean inExecutorThread() {
        return mainExecutor.inExecutorThread();
    }

    /**
     * Adds the target {@link PartitionProcessingTask} to the task queue. High priority tasks will
     * be inserted into the head of the task queue.
     */
    protected void addPartitionProcessingTask(
            PartitionProcessingTask task, boolean isPrioritizedTask) {
        try {
            final boolean triggerProcessing;
            synchronized (taskQueue) {
                triggerProcessing = taskQueue.isEmpty();
                if (isPrioritizedTask) {
                    taskQueue.addFirst(task);
                } else {
                    taskQueue.addLast(task);
                }
            }

            if (triggerProcessing) {
                mainExecutor.execute(processor);
            }
        } catch (Throwable throwable) {
            // exception may happen if too many tasks have been submitted which should be rare and
            // the corresponding consequence is not defined, so just trigger fatal error and exist
            LOG.error("Fatal: failed to add new partition processing task.", throwable);
            if (!dataStore.isShutDown()) {
                CommonUtils.exitOnFatalError(throwable);
            }
        }
    }

    /** Adds the target low priority {@link PartitionProcessingTask} to the task queue. */
    protected void addPartitionProcessingTask(PartitionProcessingTask task) {
        addPartitionProcessingTask(task, false);
    }

    @Override
    public CompletableFuture<?> releasePartition(@Nullable Throwable releaseCause) {
        CompletableFuture<?> future = new CompletableFuture<>();
        addPartitionProcessingTask(
                () -> {
                    try {
                        releaseInternal(
                                releaseCause != null
                                        ? releaseCause
                                        : new ShuffleException("Data partition released."));
                        future.complete(null);
                    } catch (Throwable throwable) {
                        future.completeExceptionally(throwable);
                    }
                },
                true);
        return future;
    }

    protected void releaseInternal(Throwable releaseCause) throws Exception {
        CommonUtils.checkArgument(releaseCause != null, "Must be not null.");
        CommonUtils.checkState(inExecutorThread(), "Not in executor thread.");

        LOG.info("Releasing data partition: {}.", getPartitionMeta());
        isReleased = true;
        if (this.releaseCause == null) {
            this.releaseCause =
                    new ShuffleException(
                            String.format("Data partition %s released.", getPartitionMeta()),
                            releaseCause);
        }

        Throwable exception = null;
        for (DataPartitionWriter writer : writers.values()) {
            try {
                writer.release(this.releaseCause);
            } catch (Throwable throwable) {
                exception = exception != null ? exception : throwable;
                LOG.error("Fatal: failed to release partition writer.", throwable);
            }
        }
        writers.clear();

        for (DataPartitionReader reader : readers) {
            try {
                reader.release(this.releaseCause);
            } catch (Throwable throwable) {
                exception = exception != null ? exception : throwable;
                LOG.error("Fatal: failed to release partition reader.", throwable);
            }
        }
        readers.clear();

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    /** Utility method to allocate buffers from the {@link BufferDispatcher}. */
    protected void allocateBuffers(
            BufferDispatcher bufferDispatcher,
            BufferListener bufferListener,
            int minBuffers,
            int maxBuffers) {
        DataPartitionMeta partitionMeta = getPartitionMeta();
        JobID jobID = partitionMeta.getJobID();
        DataSetID dataSetID = partitionMeta.getDataSetID();
        DataPartitionID partitionID = partitionMeta.getDataPartitionID();

        bufferDispatcher.requestBuffer(
                jobID, dataSetID, partitionID, minBuffers, maxBuffers, bufferListener);
    }

    /** Utility method to release this data partition when internal error occurs. */
    protected void releaseOnInternalError(Throwable throwable) throws Exception {
        releaseInternal(throwable);

        dataStore.removeDataPartition(getPartitionMeta());
    }

    /** Utility method to recycle buffers to target {@link BufferDispatcher}. */
    protected void recycleBuffers(
            Collection<ByteBuffer> buffers, BufferDispatcher bufferDispatcher) {
        DataPartitionMeta partitionMeta = getPartitionMeta();
        BufferUtils.recycleBuffers(
                buffers,
                bufferDispatcher,
                partitionMeta.getJobID(),
                partitionMeta.getDataSetID(),
                partitionMeta.getDataPartitionID());
    }

    /**
     * Returns the number of {@link DataPartitionWriter}s who have processed the input finish
     * marker.
     */
    protected int numInputFinishWriter() {
        return numInputFinishWriter;
    }

    /**
     * Increases the number of {@link DataPartitionWriter}s who have processed the input finish
     * marker. When a {@link DataPartitionWriter} is processing the input finish marker, the method
     * should be called.
     */
    protected void incNumInputFinishWriter() {
        numInputFinishWriter++;
    }

    /** Increases the number of {@link DataPartitionWriter}s who are writing data. */
    protected void incWritingCounter() {
        writingCounter++;
    }

    /** Decreases the number of {@link DataPartitionWriter}s who are writing data. */
    protected void decWritingCounter() {
        writingCounter--;
    }

    /** Returns the number of {@link DataPartitionWriter}s who are writing data. */
    protected int numWritingCounter() {
        return writingCounter;
    }

    /**
     * Returns the {@link DataPartitionWritingTask} of this data partition. Different {@link
     * DataPartition} implementations can implement different {@link DataPartitionWritingTask}.
     */
    protected abstract DataPartitionWritingTask getPartitionWritingTask();

    /**
     * Finishes the input process of this {@link DataPartition}. When calling this method, all the
     * {@link DataPartitionWriter}s have processed their input finish markers yet.
     */
    protected abstract void finishInput();

    /**
     * Returns the {@link DataPartitionReadingTask} of this data partition. Different {@link
     * DataPartition} implementations can implement different {@link DataPartitionReadingTask}.
     */
    protected abstract DataPartitionReadingTask getPartitionReadingTask();

    /** Adds the {@link DataPartitionWriter} to the queue that needs to assign buffers. */
    protected abstract void addPendingBufferWriter(DataPartitionWriter writer);

    /** Removes the {@link DataPartitionWriter} to the queue that needs to assign buffers. */
    protected abstract void removePendingBufferWriter(DataPartitionWriter writer);

    /**
     * Returns the number of {@link DataPartitionWriter}s who are waiting to be assigned buffers.
     */
    protected abstract int numPendingBufferWriters();

    /** Returns the number of {@link DataPartitionWriter}s who are waiting to be processed. */
    protected abstract int numPendingProcessWriters();

    /**
     * {@link DataPartitionProcessor} is responsible for processing all the pending {@link
     * PartitionProcessingTask}s of this {@link DataPartition}.
     */
    protected class DataPartitionProcessor implements Runnable {

        @Override
        public void run() {
            boolean continueProcessing = true;
            while (continueProcessing) {
                PartitionProcessingTask task;
                synchronized (taskQueue) {
                    task = taskQueue.pollFirst();
                    continueProcessing = !taskQueue.isEmpty();
                }

                try {
                    CommonUtils.checkNotNull(task).process();
                } catch (Throwable throwable) {
                    // tasks need to handle exceptions themselves so this should
                    // never happen, we add this to catch potential bugs
                    LOG.error("Fatal: failed to run partition processing task.", throwable);
                }
            }
        }
    }
}
