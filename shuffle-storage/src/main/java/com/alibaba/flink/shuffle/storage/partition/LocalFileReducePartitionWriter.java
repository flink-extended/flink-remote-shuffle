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

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.ReducePartition;
import com.alibaba.flink.shuffle.core.utils.ListenerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** {@link DataPartitionWriter} for {@link LocalFileReducePartition}. */
public class LocalFileReducePartitionWriter extends BaseReducePartitionWriter {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileReducePartitionWriter.class);

    /**
     * The number of credits required. The value is from the client when starting a region, it will
     * not be changed.
     */
    protected int requiredCredit;

    /**
     * The number of credits that have been satisfied. The value should not be greater than {@link
     * #requiredCredit}.
     */
    protected int fulfilledCredit;

    /** The number of map partitions corresponding to the current {@link ReducePartition}. */
    private volatile int numMaps;

    /** File writer used to write data to local file. */
    private final LocalReducePartitionFileWriter fileWriter;

    public LocalFileReducePartitionWriter(
            MapPartitionID mapPartitionID,
            BaseReducePartition dataPartition,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener,
            LocalReducePartitionFileWriter fileWriter) {
        super(mapPartitionID, dataPartition, dataRegionCreditListener, failureListener);
        this.fileWriter = fileWriter;
    }

    @Override
    public void startRegion(
            int dataRegionIndex, int numMaps, int requireCredit, boolean isBroadcastRegion) {
        super.startRegion(dataRegionIndex, numMaps, requireCredit, isBroadcastRegion);
        checkState(this.numMaps == 0 || this.numMaps == numMaps);
        this.numMaps = numMaps;
        triggerWriting();
    }

    @Override
    public void finishRegion(int dataRegionIndex) {
        super.finishRegion(dataRegionIndex);
        triggerWriting();
    }

    @Override
    public void finishDataInput(DataCommitListener commitListener) {
        super.finishDataInput(commitListener);
        triggerWriting();
    }

    @Override
    protected void processRegionStartedMarker(BufferOrMarker.RegionStartedMarker marker)
            throws Exception {
        super.processRegionStartedMarker(marker);
        isRegionFinished = false;
        requiredCredit = marker.getRequireCredit();
        fulfilledCredit = 0;
        isWritingPartial = false;

        dataPartition.incWritingCounter();
        dataPartition.addPendingBufferWriter(this);
        tryAllocatingResources();
        fileWriter.startRegion(marker.isBroadcastRegion(), marker.getMapPartitionID());
    }

    @Override
    protected void processDataBuffer(BufferOrMarker.DataBuffer buffer, boolean isLastBufferOrMarker)
            throws Exception {
        if (!fileWriter.isOpened()) {
            fileWriter.open();
        }
        checkState(dataPartition.writers.size() <= numMaps);

        isWritingPartial = true;
        tryAllocatingResources();
        // the file writer is responsible for releasing the target buffer
        fileWriter.writeBuffer(buffer, isLastBufferOrMarker);
    }

    private void tryAllocatingResources() {
        DataPartitionWritingTask writingTask =
                CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
        writingTask.allocateResources();
    }

    @Override
    protected void processRegionFinishedMarker(BufferOrMarker.RegionFinishedMarker marker)
            throws Exception {
        isRegionFinished = true;
        isWritingPartial = false;
        dataPartition.decWritingCounter();
        checkState(dataPartition.numWritingCounter() >= 0, "Wrong number of writing writers.");
        super.processRegionFinishedMarker(marker);

        fileWriter.finishRegion(marker.getMapPartitionID());
        dataPartition.removePendingBufferWriter(this);
    }

    @Override
    protected void processInputFinishedMarker(BufferOrMarker.InputFinishedMarker marker)
            throws Exception {
        checkState(availableCredits.isEmpty(), "Bug: leaking buffers.");
        checkState(isRegionFinished, "The region should be finished firstly.");
        checkState(!isWritingPartial, "The writer is writing a partial record.");

        dataPartition.incNumInputFinishWriter();
        fileWriter.prepareFinishWriting(marker);
        if (areAllWritersFinished()) {
            fileWriter.closeWriting();
            dataPartition.finishInput();
        }
        super.processInputFinishedMarker(marker);

        DataPartitionWriter removedWriter = dataPartition.writers.remove(mapPartitionID);
        checkState(removedWriter == this, "Remove a wrong writer.");
        if (dataPartition.writers.isEmpty()) {
            DataPartitionWritingTask writingTask =
                    CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
            writingTask.recycleResources();
        }
    }

    @Override
    protected void addBufferOrMarker(BufferOrMarker bufferOrMarker) {
        boolean recycleBuffer;
        boolean triggerWriting = false;

        synchronized (lock) {
            if (numPendingCredit() <= 0 && bufferOrMarkers.isEmpty()) {
                triggerWriting = true;
            }
            if (!(recycleBuffer = isReleased)) {
                bufferOrMarkers.add(bufferOrMarker);
            }

            DataPartitionWritingTask writingTask =
                    CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
            if (writingTask.hasAllocatedMaxWritingBuffers()
                    && bufferOrMarkers.size()
                                    + availableCredits.size()
                                    + fileWriter.numCacheDataBuffers()
                            == writingTask.numOccupiedBuffers()
                    && dataPartition.numWritingCounter() == dataPartition.numPendingBufferWriters()
                    && bufferOrMarkers.size() >= fileWriter.numDataBufferCacheSize() / 2) {
                triggerWriting = true;
            }
        }

        if (recycleBuffer) {
            BufferOrMarker.releaseBuffer(bufferOrMarker);
            throw new ShuffleException("Partition writer has been released.");
        }

        if (triggerWriting) {
            triggerWriting();
        }
    }

    @Override
    public void onError(Throwable throwable) {
        super.onError(throwable);
        triggerWriting();
    }

    @Override
    public boolean assignCredits(BufferQueue credits, BufferRecycler recycler) {
        CommonUtils.checkArgument(credits != null, "Must be not null.");
        CommonUtils.checkArgument(recycler != null, "Must be not null.");

        needMoreCredits = hasNotFulfilledCredit() && !isRegionFinished;
        if (isReleased || !needMoreCredits) {
            return false;
        }

        int numBuffers = Math.min(credits.size(), requiredCredit - fulfilledCredit);
        int numLeftBuffers = numBuffers;
        synchronized (lock) {
            if (isError) {
                return false;
            }

            while (numLeftBuffers-- > 0) {
                availableCredits.add(new Buffer(credits.poll(), recycler, 0));
            }
        }

        fulfilledCredit += numBuffers;
        checkState(fulfilledCredit >= 0 && fulfilledCredit <= requiredCredit);
        needMoreCredits = hasNotFulfilledCredit() && !isRegionFinished;

        ListenerUtils.notifyAvailableCredits(
                numBuffers, currentDataRegionIndex, dataRegionCreditListener);
        return needMoreCredits;
    }

    private boolean hasNotFulfilledCredit() {
        return fulfilledCredit < requiredCredit;
    }

    @Override
    public boolean isInProcessQueue() {
        return isInProcessQueue;
    }

    @Override
    public void setInProcessQueue(boolean isInProcessQueue) {
        this.isInProcessQueue = isInProcessQueue;
    }

    @Override
    public boolean isWritingPartial() {
        return isWritingPartial;
    }

    @Override
    public void triggerFlushFileDataBuffers() throws IOException {
        if (fileWriter.isOpened() && !fileWriter.isClosed()) {
            fileWriter.flushDataBuffers();
        }
    }

    @Override
    public int numPendingCredit() {
        return requiredCredit - fulfilledCredit;
    }

    private boolean areAllWritersFinished() {
        checkState(dataPartition.numInputFinishWriter() <= numMaps);
        return dataPartition.numInputFinishWriter() == numMaps;
    }

    @Override
    public void release(Throwable throwable) throws Exception {
        Throwable error = null;

        try {
            super.release(throwable);
        } catch (Throwable exception) {
            error = exception;
            LOG.error("Failed to release base partition writer.", exception);
        }

        try {
            fileWriter.close();
        } catch (Throwable exception) {
            error = error == null ? exception : error;
            LOG.error("Failed to release file writer.", exception);
        }

        if (error != null) {
            ExceptionUtils.rethrowException(error);
        }
    }

    private void triggerWriting() {
        synchronized (lock) {
            if (hasTriggeredWriting) {
                return;
            }
            DataPartitionWritingTask writingTask =
                    CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
            writingTask.triggerWriting(this);
            hasTriggeredWriting = true;
        }
    }
}
