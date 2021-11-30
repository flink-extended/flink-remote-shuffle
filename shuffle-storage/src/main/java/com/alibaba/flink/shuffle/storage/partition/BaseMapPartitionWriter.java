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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.storage.MapPartition;

/**
 * {@link BaseMapPartitionWriter} implements some basic logics for data writing of {@link
 * MapPartition}s.
 */
public abstract class BaseMapPartitionWriter extends BaseDataPartitionWriter {

    public BaseMapPartitionWriter(
            MapPartitionID mapPartitionID,
            BaseDataPartition dataPartition,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        super(dataPartition, mapPartitionID, dataRegionCreditListener, failureListener);
    }

    @Override
    protected void processRegionStartedMarker(BufferOrMarker.RegionStartedMarker marker)
            throws Exception {
        super.processRegionStartedMarker(marker);

        DataPartitionWritingTask writingTask =
                CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
        if (needMoreCredits) {
            writingTask.allocateResources();
        }
    }

    @Override
    protected void addBufferOrMarker(BufferOrMarker bufferOrMarker) {
        boolean recycleBuffer;
        boolean triggerWriting = false;

        synchronized (lock) {
            if (!(recycleBuffer = isReleased)) {
                // trigger data writing when the first buffer is added
                triggerWriting = bufferOrMarkers.isEmpty();
                bufferOrMarkers.add(bufferOrMarker);
            }
        }

        if (recycleBuffer) {
            BufferOrMarker.releaseBuffer(bufferOrMarker);
            throw new ShuffleException("Partition writer has been released.");
        }

        if (triggerWriting) {
            DataPartitionWritingTask writingTask =
                    CommonUtils.checkNotNull(dataPartition.getPartitionWritingTask());
            writingTask.triggerWriting();
        }
    }
}
