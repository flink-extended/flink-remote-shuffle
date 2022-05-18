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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutor;
import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutorServiceAdapter;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/** The utils for testing coordinators. */
public class TestingUtils {

    private static ScheduledExecutorService scheduledExecutor;

    public static synchronized ScheduledExecutor defaultScheduledExecutor() {
        if (scheduledExecutor == null || scheduledExecutor.isShutdown()) {
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        }
        return new ScheduledExecutorServiceAdapter(scheduledExecutor);
    }
}
