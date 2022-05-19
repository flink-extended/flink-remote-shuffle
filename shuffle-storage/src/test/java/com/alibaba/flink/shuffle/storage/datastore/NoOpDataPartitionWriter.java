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

import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;

/** A no-op {@link DataPartitionWriter} implementation for tests. */
public class NoOpDataPartitionWriter implements DataPartitionWriter {

    @Override
    public Buffer pollBuffer() {
        return null;
    }

    @Override
    public MapPartitionID getMapPartitionID() {
        return null;
    }

    @Override
    public boolean writeData() {
        return false;
    }

    @Override
    public void addBuffer(ReducePartitionID reducePartitionID, Buffer buffer) {}

    @Override
    public void startRegion(int dataRegionIndex, boolean isBroadcastRegion) {}

    @Override
    public void finishRegion() {}

    @Override
    public void finishDataInput(DataCommitListener commitListener) {}

    @Override
    public boolean assignCredits(BufferQueue credits, BufferRecycler recycler) {
        return false;
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public void release(Throwable throwable) {}
}
