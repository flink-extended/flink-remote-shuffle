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

package com.alibaba.flink.shuffle.coordinator.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link WorkerCheckerUtils}. */
public class WorkerCheckerUtilsTest {
    @Test
    public void testBytesToHumanReadable() {
        assertEquals(
                "5.29 E",
                WorkerCheckerUtils.bytesToHumanReadable(
                        (long) (1024L * 1024 * 1024 * 1024 * 1024 * 1024 * 5.28532)));
        assertEquals(
                "8.23 P",
                WorkerCheckerUtils.bytesToHumanReadable(
                        (long) (1024L * 1024 * 1024 * 1024 * 1024 * 8.234)));
        assertEquals(
                "1.35 T",
                WorkerCheckerUtils.bytesToHumanReadable(
                        (long) (1024L * 1024 * 1024 * 1024 * 1.346)));
        assertEquals(
                "243.40 G",
                WorkerCheckerUtils.bytesToHumanReadable((long) (1024L * 1024 * 1024 * 243.40123)));
        assertEquals(
                "1023.99 M", WorkerCheckerUtils.bytesToHumanReadable(1024 * 1024 * 1024 - 10486));
        assertEquals(
                "1024.00 M", WorkerCheckerUtils.bytesToHumanReadable(1024 * 1024 * 1024 - 1000));
        assertEquals("1.00 G", WorkerCheckerUtils.bytesToHumanReadable(1024 * 1024 * 1024 + 1));
        assertEquals("1.83 K", WorkerCheckerUtils.bytesToHumanReadable(1874));
        assertEquals("1024", WorkerCheckerUtils.bytesToHumanReadable(1024));
        assertEquals("1.00 K", WorkerCheckerUtils.bytesToHumanReadable(1025));
        assertEquals("234", WorkerCheckerUtils.bytesToHumanReadable(234));
        assertEquals("0", WorkerCheckerUtils.bytesToHumanReadable(0));
        assertEquals("-1", WorkerCheckerUtils.bytesToHumanReadable(-1));
    }

    @Test
    public void testBytesSteps() {
        assertEquals(WorkerCheckerUtils.BYTES_STEPS.length, WorkerCheckerUtils.BYTES_UNITS.length);
        assertEquals(7, WorkerCheckerUtils.BYTES_STEPS.length);
        long originalBytes = 1024;
        for (int i = 1; i < WorkerCheckerUtils.BYTES_STEPS.length; i++) {
            assertEquals(originalBytes, WorkerCheckerUtils.BYTES_STEPS[i]);
            originalBytes *= 1024;
        }
    }
}
