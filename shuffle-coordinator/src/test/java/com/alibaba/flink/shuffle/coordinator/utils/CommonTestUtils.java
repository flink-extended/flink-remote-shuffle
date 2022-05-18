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

import com.alibaba.flink.shuffle.common.functions.SupplierWithException;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.TimeoutException;

/** This class contains auxiliary methods for unit tests. */
public class CommonTestUtils {

    private static final long RETRY_INTERVAL = 100L;

    /**
     * Gets the classpath with which the current JVM was started.
     *
     * @return The classpath with which the current JVM was started.
     */
    public static String getCurrentClasspath() {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        return bean.getClassPath();
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition, long timeoutMillis)
            throws Exception {
        waitUntilCondition(condition, timeoutMillis, RETRY_INTERVAL);
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            long timeoutMillis,
            long retryIntervalMillis)
            throws Exception {
        waitUntilCondition(
                condition,
                timeoutMillis,
                retryIntervalMillis,
                "Condition was not met in given timeout.");
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            long timeoutMillis,
            String errorMsg)
            throws Exception {
        waitUntilCondition(condition, timeoutMillis, RETRY_INTERVAL, errorMsg);
    }

    public static void waitUntilCondition(
            SupplierWithException<Boolean, Exception> condition,
            long timeoutMillis,
            long retryIntervalMillis,
            String errorMsg)
            throws Exception {
        long timeLeft = timeoutMillis;
        long deadlineTime = System.nanoTime() / 1000000 + timeoutMillis;
        while (timeLeft > 0 && !condition.get()) {
            Thread.sleep(Math.min(retryIntervalMillis, timeLeft));
            timeLeft = deadlineTime - System.nanoTime() / 1000000;
        }

        if (timeLeft <= 0) {
            throw new TimeoutException(errorMsg);
        }
    }
}
