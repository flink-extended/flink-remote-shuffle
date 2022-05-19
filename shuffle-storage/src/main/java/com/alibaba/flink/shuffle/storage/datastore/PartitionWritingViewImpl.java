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

package com.alibaba.flink.shuffle.storage.datastore;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferSupplier;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;

import javax.annotation.Nullable;

/** Implementation of {@link DataPartitionWritingView}. */
public class PartitionWritingViewImpl implements DataPartitionWritingView {

    /** Target {@link DataPartitionWriter} to add data or event to. */
    private final DataPartitionWriter partitionWriter;

    /** Whether the {@link #onError} method has been called or not. */
    private boolean isError;

    /** Whether the {@link #finish} method has been called or not. */
    private boolean isInputFinished;

    /**
     * Whether the {@link #regionStarted} method has been called and a new data region has started
     * or not.
     */
    private boolean isRegionStarted;

    public PartitionWritingViewImpl(DataPartitionWriter partitionWriter) {
        CommonUtils.checkArgument(partitionWriter != null, "Must be not null.");
        this.partitionWriter = partitionWriter;
    }

    @Override
    public void onBuffer(Buffer buffer, ReducePartitionID reducePartitionID) {
        CommonUtils.checkArgument(buffer != null, "Must be not null.");

        try {
            checkNotInErrorState();
            checkInputNotFinished();
            checkRegionStarted();
            CommonUtils.checkArgument(reducePartitionID != null, "Must be not null.");
        } catch (Throwable throwable) {
            BufferUtils.recycleBuffer(buffer);
            throw throwable;
        }

        partitionWriter.addBuffer(reducePartitionID, buffer);
    }

    @Override
    public void regionStarted(int dataRegionIndex, boolean isBroadcastRegion) {
        CommonUtils.checkArgument(dataRegionIndex >= 0, "Must be non-negative.");

        checkNotInErrorState();
        checkInputNotFinished();
        checkRegionFinished();

        isRegionStarted = true;
        partitionWriter.startRegion(dataRegionIndex, isBroadcastRegion);
    }

    @Override
    public void regionFinished() {
        checkNotInErrorState();
        checkInputNotFinished();
        checkRegionStarted();

        isRegionStarted = false;
        partitionWriter.finishRegion();
    }

    @Override
    public void finish(DataCommitListener commitListener) {
        CommonUtils.checkArgument(commitListener != null, "Must be not null.");

        checkNotInErrorState();
        checkInputNotFinished();
        checkRegionFinished();

        isInputFinished = true;
        partitionWriter.finishDataInput(commitListener);
    }

    @Override
    public void onError(@Nullable Throwable throwable) {
        checkNotInErrorState();
        checkInputNotFinished();

        isError = true;
        partitionWriter.onError(throwable);
    }

    @Override
    public BufferSupplier getBufferSupplier() {
        return partitionWriter;
    }

    private void checkInputNotFinished() {
        CommonUtils.checkState(!isInputFinished, "Writing view is already finished.");
    }

    private void checkNotInErrorState() {
        CommonUtils.checkState(!isError, "Writing view is in error state.");
    }

    private void checkRegionStarted() {
        CommonUtils.checkState(isRegionStarted, "Need to start a new data region first.");
    }

    private void checkRegionFinished() {
        CommonUtils.checkState(!isRegionStarted, "Need to finish the current data region first.");
    }
}
