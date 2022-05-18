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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.core.listener.DataCommitListener;

import java.util.concurrent.CountDownLatch;

/** A {@link DataCommitListener} implementation for tests. */
public class TestDataCommitListener implements DataCommitListener {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void notifyDataCommitted() {
        latch.countDown();
    }

    public void waitForDataCommission() throws InterruptedException {
        latch.await();
    }
}
