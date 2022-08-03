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

import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;

import javax.annotation.Nullable;

/**
 * {@link DataPartitionWritingTask} encapsulates the logic of data partition writing and will be
 * triggered when there is a data writing request.
 */
public interface DataPartitionWritingTask extends PartitionProcessingTask {

    /** Allocates resources for data writing, will be called on the first data writing request. */
    void allocateResources();

    /** Triggers running of this writing task which will write data to data partition. */
    void triggerWriting(DataPartitionWriter writer);

    /**
     * Returns the maximum number of available writing buffers of this {@link
     * DataPartitionWritingTask}.
     */
    int numAvailableBuffers();

    /**
     * Returns the occupied number of available writing buffers of this {@link
     * DataPartitionWritingTask}.
     */
    int numOccupiedBuffers();

    /**
     * Whether the number of occupied buffers of this {@link DataPartitionWritingTask} equals with
     * maximum writing buffers.
     */
    boolean hasAllocatedMaxWritingBuffers();

    /**
     * Recycles the buffers of this {@link DataPartitionWritingTask}. When no more writers, it will
     * actively release the allocated resources.
     */
    void recycleResources();

    /** Releases this {@link DataPartitionWritingTask} which releases all allocated resources. */
    void release(@Nullable Throwable throwable) throws Exception;
}
