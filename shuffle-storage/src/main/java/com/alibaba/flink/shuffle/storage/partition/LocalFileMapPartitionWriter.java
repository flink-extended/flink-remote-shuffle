/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link DataPartitionWriter} for {@link LocalFileMapPartition}. */
public class LocalFileMapPartitionWriter extends BaseMapPartitionWriter {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileMapPartitionWriter.class);

    /** File writer used to write data to local file. */
    private final LocalMapPartitionFileWriter fileWriter;

    public LocalFileMapPartitionWriter(
            boolean dataChecksumEnabled,
            MapPartitionID mapPartitionID,
            BaseMapPartition dataPartition,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener,
            LocalMapPartitionFile partitionFile) {
        super(mapPartitionID, dataPartition, dataRegionCreditListener, failureListener);

        this.fileWriter =
                new LocalMapPartitionFileWriter(
                        partitionFile,
                        dataPartition.getPartitionWritingTask().minBuffersToWrite,
                        dataChecksumEnabled);
    }

    @Override
    protected void processRegionStartedMarker(BufferOrMarker.RegionStartedMarker marker)
            throws Exception {
        super.processRegionStartedMarker(marker);

        fileWriter.startRegion(marker.isBroadcastRegion());
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
    protected void processRegionFinishedMarker(BufferOrMarker.RegionFinishedMarker marker)
            throws Exception {
        super.processRegionFinishedMarker(marker);

        fileWriter.finishRegion();
    }

    @Override
    protected void processInputFinishedMarker(BufferOrMarker.InputFinishedMarker marker)
            throws Exception {
        fileWriter.finishWriting();

        super.processInputFinishedMarker(marker);
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
}
