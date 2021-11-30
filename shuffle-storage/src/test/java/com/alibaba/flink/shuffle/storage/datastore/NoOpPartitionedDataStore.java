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

package com.alibaba.flink.shuffle.storage.datastore;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.executor.SimpleSingleThreadExecutorPool;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutorPool;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import javax.annotation.Nullable;

import java.util.Properties;

/** A no-op {@link PartitionedDataStore} implementation for tests. */
public class NoOpPartitionedDataStore implements PartitionedDataStore {

    private final BufferDispatcher writingBufferDispatcher =
            new BufferDispatcher(
                    "Test Writing Buffer Pool", 1024, StorageTestUtils.DATA_BUFFER_SIZE);

    private final BufferDispatcher readingBufferDispatcher =
            new BufferDispatcher(
                    "Test Reading Buffer Pool", 1024, StorageTestUtils.DATA_BUFFER_SIZE);

    private final SingleThreadExecutorPool executorPool =
            new SimpleSingleThreadExecutorPool(4, "Test Executor Pool");

    @Override
    public DataPartitionWritingView createDataPartitionWritingView(WritingViewContext context) {
        return null;
    }

    @Override
    public DataPartitionReadingView createDataPartitionReadingView(ReadingViewContext context) {
        return null;
    }

    @Override
    public boolean isDataPartitionConsumable(DataPartitionMeta partitionMeta) {
        return false;
    }

    @Override
    public void addDataPartition(DataPartitionMeta partitionMeta) {}

    @Override
    public void removeDataPartition(DataPartitionMeta partitionMeta) {}

    @Override
    public void releaseDataPartition(
            DataSetID dataSetID, DataPartitionID partitionID, @Nullable Throwable throwable) {}

    @Override
    public void releaseDataSet(DataSetID dataSetID, @Nullable Throwable throwable) {}

    @Override
    public void releaseDataByJobID(JobID jobID, @Nullable Throwable throwable) {}

    @Override
    public void shutDown(boolean releaseData) {}

    @Override
    public boolean isShutDown() {
        return false;
    }

    @Override
    public Configuration getConfiguration() {
        return new Configuration(new Properties());
    }

    @Override
    public BufferDispatcher getWritingBufferDispatcher() {
        return writingBufferDispatcher;
    }

    @Override
    public BufferDispatcher getReadingBufferDispatcher() {
        return readingBufferDispatcher;
    }

    @Override
    public SingleThreadExecutorPool getExecutorPool(StorageMeta storageMeta) {
        return executorPool;
    }
}
