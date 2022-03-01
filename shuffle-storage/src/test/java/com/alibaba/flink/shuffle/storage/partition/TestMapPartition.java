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

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionStatistics;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.MapPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import javax.annotation.Nullable;

import java.util.Properties;

/** A fake {@link DataPartition} implementation for tests. */
public class TestMapPartition extends BaseMapPartition {

    private final TestPartitionWritingTask writingTask;

    public TestMapPartition(PartitionedDataStore dataStore) {
        super(
                dataStore,
                dataStore
                        .getExecutorPool(StorageTestUtils.getStorageMeta())
                        .getSingleThreadExecutor());

        this.writingTask = new TestPartitionWritingTask(new Configuration(new Properties()));
    }

    @Override
    public MapPartitionMeta getPartitionMeta() {
        return null;
    }

    @Override
    public DataPartitionType getPartitionType() {
        return null;
    }

    @Override
    public boolean isConsumable() {
        return false;
    }

    @Override
    public DataPartitionStatistics getDataPartitionStatistics() {
        return null;
    }

    @Override
    protected DataPartitionReader getDataPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        return null;
    }

    @Override
    protected DataPartitionWriter getDataPartitionWriter(
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        return null;
    }

    @Override
    public TestPartitionWritingTask getPartitionWritingTask() {
        return writingTask;
    }

    @Override
    public MapPartitionReadingTask getPartitionReadingTask() {
        return null;
    }

    /** A fake {@link DataPartitionWritingTask} implementation for tests. */
    final class TestPartitionWritingTask extends BaseMapPartition.MapPartitionWritingTask {

        private int numWritingTriggers;

        protected TestPartitionWritingTask(Configuration configuration) {
            super(configuration);
        }

        @Override
        public void allocateResources() {}

        @Override
        public void triggerWriting() {
            ++numWritingTriggers;
        }

        @Override
        public void release(@Nullable Throwable throwable) {}

        @Override
        public void process() {}

        public int getNumWritingTriggers() {
            return numWritingTriggers;
        }
    }
}
