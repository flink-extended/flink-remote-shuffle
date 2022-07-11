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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Data buffer or event markers. It is just a helper structure can be used by {@link DataPartition}
 * implementations.
 */
public abstract class BufferOrMarker {

    private static final Logger LOG = LoggerFactory.getLogger(BufferOrMarker.class);

    /** Marks the producer partition of this buffer or marker. */
    private final MapPartitionID mapPartitionID;

    public BufferOrMarker(MapPartitionID mapPartitionID) {
        CommonUtils.checkArgument(mapPartitionID != null, "Must be not null.");
        this.mapPartitionID = mapPartitionID;
    }

    public abstract Type getType();

    public MapPartitionID getMapPartitionID() {
        return mapPartitionID;
    }

    public DataBuffer asDataBuffer() {
        return (DataBuffer) this;
    }

    public RegionStartedMarker asRegionStartedMarker() {
        return (RegionStartedMarker) this;
    }

    public RegionFinishedMarker asRegionFinishedMarker() {
        return (RegionFinishedMarker) this;
    }

    public InputFinishedMarker asInputFinishedMarker() {
        return (InputFinishedMarker) this;
    }

    public ErrorMarker asErrorMarker() {
        return (ErrorMarker) this;
    }

    /** Releases all the given {@link BufferOrMarker}s. Will log error if any exception occurs. */
    public static void releaseBuffers(
            @Nullable Collection<? extends BufferOrMarker> bufferOrMarkers) {
        if (bufferOrMarkers == null) {
            return;
        }

        for (BufferOrMarker bufferOrMarker : bufferOrMarkers) {
            releaseBuffer(bufferOrMarker);
        }
        // clear method is not supported by all collections
        CommonUtils.runQuietly(bufferOrMarkers::clear);
    }

    /** Releases the target {@link BufferOrMarker}. Will log error if any exception occurs. */
    public static void releaseBuffer(@Nullable BufferOrMarker bufferOrMarker) {
        if (bufferOrMarker == null) {
            return;
        }

        try {
            if (bufferOrMarker.getType() == Type.DATA_BUFFER) {
                BufferUtils.recycleBuffer(bufferOrMarker.asDataBuffer().getBuffer());
            }
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release the target buffer.", throwable);
        }
    }

    /** Types of {@link BufferOrMarker}. */
    enum Type {

        /** <b>DATA_BUFFER</b> represents a {@link Buffer} containing data to be written. */
        DATA_BUFFER,

        /**
         * <b>REGION_STARTED_MARKER</b> marks the starting of a new data region in the writing
         * stream.
         */
        REGION_STARTED_MARKER,

        /**
         * <b>REGION_FINISHED_MARKER</b> marks the ending of current data region in the writing
         * stream.
         */
        REGION_FINISHED_MARKER,

        /** <b>REGION_FINISHED_MARKER</b> marks the ending of the current writing stream. */
        INPUT_FINISHED_MARKER,

        /** <b>ERROR_MARKER</b> marks the failure of the corresponding data writing view. */
        ERROR_MARKER
    }

    /** Definition of {@link Type#DATA_BUFFER}. */
    public static class DataBuffer extends BufferOrMarker {

        /** Target reduce partition of the data. */
        private final ReducePartitionID reducePartitionID;

        /** Region index of the data. */
        private final int dataRegionIndex;

        /** Buffer containing data to be written. */
        private final Buffer buffer;

        public DataBuffer(
                MapPartitionID mapPartitionID,
                int dataRegionIndex,
                ReducePartitionID reducePartitionID,
                Buffer buffer) {
            super(mapPartitionID);

            CommonUtils.checkArgument(reducePartitionID != null, "Must be not null.");
            CommonUtils.checkArgument(buffer != null, "Must be not null.");
            CommonUtils.checkArgument(dataRegionIndex >= 0, "Must be non-negative.");

            this.reducePartitionID = reducePartitionID;
            this.dataRegionIndex = dataRegionIndex;
            this.buffer = buffer;
        }

        @Override
        public Type getType() {
            return Type.DATA_BUFFER;
        }

        public void release() {
            buffer.release();
        }

        public int getDataRegionIndex() {
            return dataRegionIndex;
        }

        public ReducePartitionID getReducePartitionID() {
            return reducePartitionID;
        }

        public Buffer getBuffer() {
            return buffer;
        }
    }

    /** Definition of {@link Type#REGION_STARTED_MARKER}. */
    public static class RegionStartedMarker extends BufferOrMarker {

        /** Data region index (started from 0) of the new region. */
        private final int dataRegionIndex;

        private final int requireCredit;

        /**
         * Whether the new data region is a broadcast region. In a broadcast region, each piece of
         * data will be written to all reduce partitions.
         */
        private final boolean isBroadcastRegion;

        public RegionStartedMarker(
                MapPartitionID mapPartitionID,
                int dataRegionIndex,
                int requireCredit,
                boolean isBroadcastRegion) {
            super(mapPartitionID);

            CommonUtils.checkArgument(dataRegionIndex >= 0, "Must be non-negative.");

            this.dataRegionIndex = dataRegionIndex;
            this.requireCredit = requireCredit;
            this.isBroadcastRegion = isBroadcastRegion;
        }

        @Override
        public Type getType() {
            return Type.REGION_STARTED_MARKER;
        }

        public int getDataRegionIndex() {
            return dataRegionIndex;
        }

        public int getRequireCredit() {
            return requireCredit;
        }

        public boolean isBroadcastRegion() {
            return isBroadcastRegion;
        }
    }

    /** Definition of {@link Type#REGION_FINISHED_MARKER}. */
    public static class RegionFinishedMarker extends BufferOrMarker {

        /** Data region index (started from 0) of the new region. */
        private final int dataRegionIndex;

        public RegionFinishedMarker(MapPartitionID mapPartitionID, int dataRegionIndex) {
            super(mapPartitionID);

            CommonUtils.checkArgument(dataRegionIndex >= 0, "Must be non-negative.");

            this.dataRegionIndex = dataRegionIndex;
        }

        @Override
        public Type getType() {
            return Type.REGION_FINISHED_MARKER;
        }

        public int getDataRegionIndex() {
            return dataRegionIndex;
        }
    }

    /** Definition of {@link Type#INPUT_FINISHED_MARKER}. */
    public static class InputFinishedMarker extends BufferOrMarker {

        /** Listener to be notified when all the data written is committed. */
        private final DataCommitListener commitListener;

        public InputFinishedMarker(
                MapPartitionID mapPartitionID, DataCommitListener commitListener) {
            super(mapPartitionID);

            CommonUtils.checkArgument(commitListener != null, "Must be not null.");
            this.commitListener = commitListener;
        }

        public DataCommitListener getCommitListener() {
            return commitListener;
        }

        @Override
        public Type getType() {
            return Type.INPUT_FINISHED_MARKER;
        }
    }

    /** Definition of {@link Type#ERROR_MARKER}. */
    public static class ErrorMarker extends BufferOrMarker {

        /** Failure encountered in the corresponding writing view. */
        private final Throwable throwable;

        public ErrorMarker(MapPartitionID mapPartitionID, Throwable throwable) {
            super(mapPartitionID);

            CommonUtils.checkArgument(throwable != null, "Must be not null.");
            this.throwable = throwable;
        }

        public Throwable getFailure() {
            return throwable;
        }

        @Override
        public Type getType() {
            return Type.ERROR_MARKER;
        }
    }
}
