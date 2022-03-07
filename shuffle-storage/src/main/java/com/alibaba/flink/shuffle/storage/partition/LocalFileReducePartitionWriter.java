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
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.utils.ListenerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** {@link DataPartitionWriter} for {@link LocalFileReducePartition}. */
public class LocalFileReducePartitionWriter extends BaseReducePartitionWriter {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileReducePartitionWriter.class);

    protected int requiredCredit;

    protected int fulfilledCredit;

    private boolean isInputFinished;

    private int numMaps;

    private BufferOrMarker.InputFinishedMarker inputFinishedMarker;

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
        this.isInputFinished = false;
    }

    @Override
    public void addBuffer(ReducePartitionID reducePartitionID, int dataRegionIndex, Buffer buffer) {
        LOG.debug(
                "Receive addBuffer, {}, id: {}, {}, regionID={}",
                dataPartition,
                dataPartition.getPartitionMeta().getDataPartitionID(),
                mapPartitionID,
                dataRegionIndex);
        super.addBuffer(reducePartitionID, dataRegionIndex, buffer);
    }

    @Override
    public void startRegion(
            int dataRegionIndex, int numMaps, int inputRequireCredit, boolean isBroadcastRegion) {
        super.startRegion(dataRegionIndex, numMaps, inputRequireCredit, isBroadcastRegion);
        this.numMaps = numMaps;

        triggerWriting();
    }

    @Override
    public void finishRegion(int dataRegionIndex) {
        super.finishRegion(dataRegionIndex);
        needMoreCredits = false;
        triggerWriting();
    }

    @Override
    public void finishDataInput(DataCommitListener commitListener) {
        super.finishDataInput(commitListener);
        triggerWriting();
    }

    @Override
    public void finishPartitionInput() throws Exception {
        checkState(inputFinishedMarker != null, "Empty input finish marker.");
        super.processInputFinishedMarker(inputFinishedMarker);
    }

    @Override
    protected void processRegionStartedMarker(BufferOrMarker.RegionStartedMarker marker)
            throws Exception {
        super.processRegionStartedMarker(marker);
        isRegionFinished = false;
        requiredCredit = marker.getRequireCredit();
        fulfilledCredit = 0;
        dataPartition.addPendingBufferWriter(this);
        DataPartitionWritingTask writingTask =
                CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
        if (needMoreCredits) {
            writingTask.allocateResources();
        }
        fileWriter.startRegion(marker.isBroadcastRegion(), marker.getMapPartitionID());
    }

    @Override
    protected void processDataBuffer(BufferOrMarker.DataBuffer buffer) throws Exception {
        if (!fileWriter.isOpened()) {
            fileWriter.open();
        }

        // the file writer is responsible for releasing the target buffer
        fileWriter.writeBuffer(buffer);
    }

    @Override
    public Buffer pollBuffer() {
        synchronized (lock) {
            if (isReleased || isError) {
                throw new ShuffleException("Partition writer has been released or failed.");
            }

            Buffer buffer = availableCredits.poll();
            if (dataPartition.numWritingCounter() == 1
                    && dataPartition.numWritingTaskBuffers() == 0
                    && availableCredits.isEmpty()) {
                DataPartitionWritingTask writingTask =
                        CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
                writingTask.triggerWriting();
            }
            return buffer;
        }
    }

    @Override
    protected void processRegionFinishedMarker(BufferOrMarker.RegionFinishedMarker marker)
            throws Exception {
        isRegionFinished = true;
        super.processRegionFinishedMarker(marker);

        fileWriter.finishRegion(marker.getMapPartitionID());
        dataPartition.decWritingCount();
    }

    @Override
    protected void processInputFinishedMarker(BufferOrMarker.InputFinishedMarker marker)
            throws Exception {
        fileWriter.prepareFinishWriting(marker);

        checkState(availableCredits.isEmpty(), "Bug: leaking buffers.");
        checkState(
                isRegionFinished,
                "The region should be stopped first before the input is finished.");
        isInputFinished = true;
        checkState(inputFinishedMarker == null, "Duplicated input finish marker.");
        inputFinishedMarker = marker;
        if (areAllWritersFinished()) {
            DataPartitionWritingTask writingTask =
                    CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
            writingTask.recycleResources();
            fileWriter.closeWriting();
            writingTask.finishInput();
        }
    }

    @Override
    protected void addBufferOrMarker(BufferOrMarker bufferOrMarker) {
        boolean recycleBuffer;

        synchronized (lock) {
            if (!(recycleBuffer = isReleased)) {
                bufferOrMarkers.add(bufferOrMarker);
            }
        }

        if (recycleBuffer) {
            BufferOrMarker.releaseBuffer(bufferOrMarker);
            throw new ShuffleException("Partition writer has been released.");
        }
    }

    @Override
    public boolean assignCredits(
            BufferQueue credits, BufferRecycler recycler, boolean checkMinBuffers) {
        CommonUtils.checkArgument(credits != null, "Must be not null.");
        CommonUtils.checkArgument(recycler != null, "Must be not null.");

        needMoreCredits = !isCreditFulfilled() && !isRegionFinished;
        if (isReleased || isCreditFulfilled()) {
            return false;
        }

        if (checkMinBuffers && credits.size() < MIN_CREDITS_TO_NOTIFY) {
            return false;
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

        fulfilledCredit += numBuffers;

        ListenerUtils.notifyAvailableCredits(
                numBuffers, currentDataRegionIndex, dataRegionCreditListener);
        needMoreCredits = !isCreditFulfilled() && !isRegionFinished;
        return needMoreCredits;
    }

    @Override
    public boolean isCreditFulfilled() {
        return fulfilledCredit >= requiredCredit;
    }

    @Override
    public int numPendingCredit() {
        return requiredCredit - fulfilledCredit;
    }

    @Override
    public int numFulfilledCredit() {
        return fulfilledCredit;
    }

    @Override
    public boolean isInputFinished() {
        return isInputFinished;
    }

    @Override
    public boolean isRegionFinished() {
        return isRegionFinished;
    }

    private boolean areAllWritersFinished() {
        int numInputFinish = 0;
        for (Map.Entry<MapPartitionID, DataPartitionWriter> writerEntry :
                dataPartition.writers.entrySet()) {
            if (!writerEntry.getValue().isInputFinished()) {
                return false;
            }
            numInputFinish++;
        }
        if (numInputFinish < numMaps) {
            return false;
        }
        return true;
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

    @Override
    protected Queue<BufferOrMarker> getPendingBufferOrMarkers() {
        synchronized (lock) {
            if (bufferOrMarkers.isEmpty()) {
                return null;
            }

            BufferOrMarker.Type type = bufferOrMarkers.getLast().getType();
            boolean shouldWriteData =
                    type == BufferOrMarker.Type.REGION_STARTED_MARKER
                            || type == BufferOrMarker.Type.REGION_FINISHED_MARKER
                            || type == BufferOrMarker.Type.INPUT_FINISHED_MARKER;
            if (!shouldWriteData) {
                return null;
            }

            Queue<BufferOrMarker> pendingBufferOrMarkers = new ArrayDeque<>(bufferOrMarkers);
            bufferOrMarkers.clear();
            return pendingBufferOrMarkers;
        }
    }

    private void triggerWriting() {
        DataPartitionWritingTask writingTask =
                CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
        writingTask.triggerWriting();
    }
}
