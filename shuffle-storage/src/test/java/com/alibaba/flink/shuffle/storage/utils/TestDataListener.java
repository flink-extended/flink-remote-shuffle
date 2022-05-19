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

import com.alibaba.flink.shuffle.core.listener.DataListener;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/** A {@link DataListener} implementation for tests. */
public class TestDataListener implements DataListener {

    private final BlockingQueue<Object> notifications = new LinkedBlockingQueue<>();

    @Override
    public void notifyDataAvailable() {
        notifications.add(new Object());
    }

    public Object waitData(long timeout) throws InterruptedException {
        return notifications.poll(timeout, TimeUnit.MILLISECONDS);
    }
}
