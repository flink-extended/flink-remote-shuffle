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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.core.listener.PartitionStateListener;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;

import java.util.concurrent.atomic.AtomicInteger;

/** A {@link PartitionStateListener} implementation for tests. */
public class TestPartitionStateListener implements PartitionStateListener {

    private final AtomicInteger numCreated = new AtomicInteger();

    private final AtomicInteger numRemoved = new AtomicInteger();

    @Override
    public void onPartitionCreated(DataPartitionMeta partitionMeta) {
        numCreated.incrementAndGet();
    }

    @Override
    public void onPartitionRemoved(DataPartitionMeta partitionMeta) {
        numRemoved.incrementAndGet();
    }

    public int getNumCreated() {
        return numCreated.get();
    }

    public int getNumRemoved() {
        return numRemoved.get();
    }
}
