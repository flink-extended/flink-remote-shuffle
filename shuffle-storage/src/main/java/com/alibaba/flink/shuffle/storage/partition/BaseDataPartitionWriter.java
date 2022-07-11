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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;
import com.alibaba.flink.shuffle.core.utils.ListenerUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Queue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * {@link BaseDataPartitionWriter} implements some basics logic of {@link DataPartitionWriter} which
 * can be reused by subclasses and simplify the implementation of new {@link DataPartitionWriter}s.
 */
public abstract class BaseDataPartitionWriter implements DataPartitionWriter {

    /**
     * Minimum number of credits to notify the credit listener of new credits. Bulk notification can
     * reduce small network packages.
     */
    public static final int MIN_CREDITS_TO_NOTIFY = 10;

    /** Target {@link DataPartition} to write data to. */
    protected final BaseDataPartition dataPartition;

    /** {@link MapPartitionID} of all the data written. */
    protected final MapPartitionID mapPartitionID;

    /**
     * {@link DataRegionCreditListener} to be notified when new credits are available for the
     * corresponding data producer.
     */
    protected final DataRegionCreditListener dataRegionCreditListener;

    /**
     * {@link FailureListener} to be notified if any exception occurs when processing the pending
     * {@link BufferOrMarker}s.
     */
    protected final FailureListener failureListener;

    /**
     * Lock used for synchronization and to avoid potential race conditions between the data writing
     * thread and the data partition executor thread.
     */
    protected final Object lock = new Object();

    /** All available credits can be used by the corresponding data producer. */
    @GuardedBy("lock")
    protected final Queue<Buffer> availableCredits = new ArrayDeque<>();

    /**
     * All pending {@link BufferOrMarker}s already added to this partition writer and waiting to be
     * processed.
     */
    @GuardedBy("lock")
    protected final Deque<BufferOrMarker> bufferOrMarkers = new ArrayDeque<>();

    /** Whether this partition writer has been released or not. */
    @GuardedBy("lock")
    protected boolean isReleased;

    /** Whether there is any error at the producer side or not. */
    @GuardedBy("lock")
    protected boolean isError;

    /**
     * Whether this {@link DataPartitionWriter} needs more credits to receive and cache data or not.
     */
    protected boolean needMoreCredits;

    /** Whether this {@link DataPartitionWriter} has processed the region finish marker. */
    protected boolean isRegionFinished;

    /**
     * Whether this {@link DataPartitionWriter} is writing partial data to file writer. If true, do
     * not poll the writer out from the pending process queue of the {@link BaseReducePartition}.
     */
    protected boolean isWritingPartial;

    protected boolean hasTriggeredWriting;

    /**
     * Whether this {@link DataPartitionWriter} is in the pending process queue of the {@link
     * BaseReducePartition}.
     */
    protected boolean isInProcessQueue;

    /** Index number of the current data region being written. */
    protected int currentDataRegionIndex;

    protected BaseDataPartitionWriter(
            BaseDataPartition dataPartition,
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        CommonUtils.checkArgument(dataPartition != null, "Must be not null.");
        CommonUtils.checkArgument(mapPartitionID != null, "Must be not null.");
        CommonUtils.checkArgument(dataRegionCreditListener != null, "Must be not null.");
        CommonUtils.checkArgument(failureListener != null, "Must be not null.");

        this.dataPartition = dataPartition;
        this.mapPartitionID = mapPartitionID;
        this.dataRegionCreditListener = dataRegionCreditListener;
        this.failureListener = failureListener;
    }

    @Override
    public MapPartitionID getMapPartitionID() {
        return mapPartitionID;
    }

    @Override
    public void addBuffer(ReducePartitionID reducePartitionID, int dataRegionIndex, Buffer buffer) {
        addBufferOrMarker(
                new BufferOrMarker.DataBuffer(
                        mapPartitionID, dataRegionIndex, reducePartitionID, buffer));
    }

    @Override
    public void startRegion(int dataRegionIndex, boolean isBroadcastRegion) {
        startRegion(dataRegionIndex, 1, 0, isBroadcastRegion);
    }

    @Override
    public void startRegion(
            int dataRegionIndex, int numMaps, int requireCredit, boolean isBroadcastRegion) {
        addBufferOrMarker(
                new BufferOrMarker.RegionStartedMarker(
                        mapPartitionID, dataRegionIndex, requireCredit, isBroadcastRegion));
    }

    @Override
    public void finishRegion(int dataRegionIndex) {
        addBufferOrMarker(new BufferOrMarker.RegionFinishedMarker(mapPartitionID, dataRegionIndex));
    }

    @Override
    public void finishDataInput(DataCommitListener commitListener) {
        addBufferOrMarker(new BufferOrMarker.InputFinishedMarker(mapPartitionID, commitListener));
    }

    /** Adds a new {@link BufferOrMarker} to this partition writer to be processed. */
    protected abstract void addBufferOrMarker(BufferOrMarker bufferOrMarker);

    @Override
    public boolean writeData() throws Exception {
        Queue<BufferOrMarker> pendingBufferOrMarkers = getPendingBufferOrMarkers();
        if (pendingBufferOrMarkers == null) {
            return false;
        }

        BufferOrMarker bufferOrMarker;
        try {
            while ((bufferOrMarker = pendingBufferOrMarkers.poll()) != null) {
                if (processBufferOrMarker(bufferOrMarker, pendingBufferOrMarkers.isEmpty())) {
                    return true;
                }
            }
        } finally {
            BufferOrMarker.releaseBuffers(pendingBufferOrMarkers);
        }
        return false;
    }

    protected boolean processBufferOrMarker(
            BufferOrMarker bufferOrMarker, boolean isLastBufferOrMarker) throws Exception {
        switch (bufferOrMarker.getType()) {
            case ERROR_MARKER:
                processErrorMarker(bufferOrMarker.asErrorMarker());
                return true;
            case INPUT_FINISHED_MARKER:
                processInputFinishedMarker(bufferOrMarker.asInputFinishedMarker());
                return true;
            case REGION_STARTED_MARKER:
                processRegionStartedMarker(bufferOrMarker.asRegionStartedMarker());
                return false;
            case REGION_FINISHED_MARKER:
                processRegionFinishedMarker(bufferOrMarker.asRegionFinishedMarker());
                return false;
            case DATA_BUFFER:
                processDataBuffer(bufferOrMarker.asDataBuffer(), isLastBufferOrMarker);
                return false;
            default:
                throw new ShuffleException(
                        String.format("Illegal type: %s.", bufferOrMarker.getType()));
        }
    }

    protected void processErrorMarker(BufferOrMarker.ErrorMarker marker) throws Exception {
        needMoreCredits = false;
        releaseUnusedCredits();
        ExceptionUtils.rethrowException(marker.getFailure());
    }

    protected void processRegionStartedMarker(BufferOrMarker.RegionStartedMarker marker)
            throws Exception {
        needMoreCredits = true;
        currentDataRegionIndex = marker.getDataRegionIndex();
    }

    protected abstract void processDataBuffer(
            BufferOrMarker.DataBuffer buffer, boolean isLastBufferOrMarker) throws Exception;

    protected void processRegionFinishedMarker(BufferOrMarker.RegionFinishedMarker marker)
            throws Exception {
        needMoreCredits = false;
        releaseUnusedCredits();
    }

    protected void processInputFinishedMarker(BufferOrMarker.InputFinishedMarker marker)
            throws Exception {
        checkState(!needMoreCredits, "Must finish region before finish input.");
        checkState(availableCredits.isEmpty(), "Buffers (credits) leaking.");
        ListenerUtils.notifyDataCommitted(marker.getCommitListener());
    }

    @Override
    public void onError(Throwable throwable) {
        synchronized (lock) {
            if (isReleased || isError) {
                return;
            }

            isError = true;
        }

        Queue<BufferOrMarker> pendingBufferOrMarkers = getPendingBufferOrMarkers();
        BufferOrMarker.releaseBuffers(pendingBufferOrMarkers);

        Throwable exception = new ShuffleException("Writing view failed.", throwable);
        addBufferOrMarker(new BufferOrMarker.ErrorMarker(mapPartitionID, exception));
    }

    @Override
    public boolean assignCredits(BufferQueue credits, BufferRecycler recycler) {
        CommonUtils.checkArgument(credits != null, "Must be not null.");
        CommonUtils.checkArgument(recycler != null, "Must be not null.");

        if (isReleased || !needMoreCredits) {
            return false;
        }

        if (credits.size() < MIN_CREDITS_TO_NOTIFY) {
            return needMoreCredits;
        }

        int numBuffers = 0;
        synchronized (lock) {
            if (isError) {
                return false;
            }

            while (credits.size() > 0) {
                ++numBuffers;
                availableCredits.add(new Buffer(credits.poll(), recycler, 0));
            }
        }

        ListenerUtils.notifyAvailableCredits(
                numBuffers, currentDataRegionIndex, dataRegionCreditListener);
        return needMoreCredits;
    }

    @Override
    public boolean isInProcessQueue() {
        return false;
    }

    @Override
    public void setInProcessQueue(boolean isInProcessQueue) {}

    @Override
    public boolean isWritingPartial() {
        return false;
    }

    @Override
    public void triggerFlushFileDataBuffers() throws IOException {}

    @Override
    public int numBufferOrMarkers() {
        return bufferOrMarkers.size();
    }

    @Override
    public int numPendingCredit() {
        return 0;
    }

    @Override
    public Buffer pollBuffer() {
        synchronized (lock) {
            if (isReleased || isError) {
                throw new ShuffleException("Partition writer has been released or failed.");
            }

            return availableCredits.poll();
        }
    }

    @Override
    public void release(Throwable throwable) throws Exception {
        Queue<Buffer> buffers;
        boolean notifyFailure;

        synchronized (lock) {
            if (isReleased) {
                return;
            }

            notifyFailure = !isError;
            isReleased = true;
            buffers = new ArrayDeque<>(availableCredits);
            availableCredits.clear();
        }

        if (notifyFailure) {
            ListenerUtils.notifyFailure(
                    failureListener,
                    new ShuffleException(
                            "Error encountered while writing data partition.", throwable));
        }

        BufferUtils.recycleBuffers(buffers);
        BufferOrMarker.releaseBuffers(getPendingBufferOrMarkers());
        releaseUnusedCredits();
    }

    protected void releaseUnusedCredits() {
        Queue<Buffer> unusedCredits;
        synchronized (lock) {
            unusedCredits = new ArrayDeque<>(availableCredits);
            availableCredits.clear();
        }

        BufferUtils.recycleBuffers(unusedCredits);
    }

    protected Queue<BufferOrMarker> getPendingBufferOrMarkers() {
        synchronized (lock) {
            hasTriggeredWriting = false;
            if (bufferOrMarkers.isEmpty()) {
                return null;
            }

            Queue<BufferOrMarker> pendingBufferOrMarkers = new ArrayDeque<>(bufferOrMarkers);
            bufferOrMarkers.clear();
            return pendingBufferOrMarkers;
        }
    }
}
