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

import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;

/** A no-op {@link DataPartitionReader} implementation for tests. */
public class NoOpDataPartitionReader implements DataPartitionReader {

    @Override
    public void open() {}

    @Override
    public boolean readData(BufferQueue buffers, BufferRecycler recycler) {
        return false;
    }

    @Override
    public BufferWithBacklog nextBuffer() {
        return null;
    }

    @Override
    public void release(Throwable throwable) {}

    @Override
    public boolean isFinished() {
        return false;
    }

    @Override
    public long getPriority() {
        return 0;
    }

    @Override
    public void onError(Throwable throwable) {}

    @Override
    public boolean isOpened() {
        return false;
    }
}
