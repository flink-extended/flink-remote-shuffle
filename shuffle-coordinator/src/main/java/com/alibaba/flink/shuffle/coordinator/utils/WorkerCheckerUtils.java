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

import com.alibaba.flink.shuffle.coordinator.worker.checker.ShuffleWorkerChecker;

/** Utility methods to manipulate {@link ShuffleWorkerChecker}s. */
public class WorkerCheckerUtils {
    private static final long Kb = 1024;
    private static final long Mb = Kb * 1024;
    private static final long Gb = Mb * 1024;
    private static final long Tb = Gb * 1024;
    private static final long Pb = Tb * 1024;
    private static final long Eb = Pb * 1024;

    protected static final String[] BYTES_UNITS = new String[] {"", "K", "M", "G", "T", "P", "E"};

    protected static final long[] BYTES_STEPS = new long[] {0, Kb, Mb, Gb, Tb, Pb, Eb};

    public static String bytesToHumanReadable(long bytes) {
        for (int i = BYTES_UNITS.length - 1; i > 0; i--) {
            if (bytes > BYTES_STEPS[i]) {
                return String.format("%3.2f %s", (double) bytes / BYTES_STEPS[i], BYTES_UNITS[i]);
            }
        }
        return Long.toString(bytes);
    }
}
