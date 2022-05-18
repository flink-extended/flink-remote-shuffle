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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;

import java.util.ArrayDeque;
import java.util.Queue;

/** A {@link DataRegionCreditListener} implementation for tests. */
public class TestDataRegionCreditListener implements DataRegionCreditListener {

    private final Queue<Object> buffers = new ArrayDeque<>();

    private int currentDataRegion = -1;

    @Override
    public void notifyCredits(int availableCredits, int dataRegionIndex) {
        CommonUtils.checkState(availableCredits > 0, "Must be positive.");
        synchronized (buffers) {
            if (dataRegionIndex != currentDataRegion) {
                currentDataRegion = dataRegionIndex;
                buffers.clear();
            }

            while (availableCredits > 0) {
                buffers.add(new Object());
                --availableCredits;
            }
            buffers.notify();
        }
    }

    public Object take(long timeout, int dataRegionIndex) throws InterruptedException {
        synchronized (buffers) {
            while (buffers.isEmpty() || dataRegionIndex != currentDataRegion) {
                buffers.wait(timeout);
                if (timeout > 0) {
                    break;
                }
            }
            return buffers.poll();
        }
    }
}
