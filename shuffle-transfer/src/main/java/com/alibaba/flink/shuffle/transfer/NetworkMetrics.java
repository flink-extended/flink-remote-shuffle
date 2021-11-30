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

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.metrics.entry.MetricUtils;

import com.alibaba.metrics.Counter;
import com.alibaba.metrics.Meter;

/** Constants and util methods of network metrics. */
public class NetworkMetrics {

    // Group name
    public static final String NETWORK = "remote-shuffle.network";

    // Current number of tcp writing connections.
    public static final String NUM_WRITING_CONNECTIONS = NETWORK + ".num_writing_connections";

    // Current number of tcp reading connections.
    public static final String NUM_READING_CONNECTIONS = NETWORK + ".num_reading_connections";

    // Current number of writing flows.
    public static final String NUM_WRITING_FLOWS = NETWORK + ".num_writing_flows";

    // Current number of reading flows.
    public static final String NUM_READING_FLOWS = NETWORK + ".num_reading_flows";

    // Current writing throughput in bytes.
    public static final String NUM_BYTES_WRITING_THROUGHPUT = NETWORK + ".writing_throughput_bytes";

    // Current reading throughput in bytes.
    public static final String NUM_BYTES_READING_THROUGHPUT = NETWORK + ".reading_throughput_bytes";

    public static Counter numWritingConnections() {
        return MetricUtils.getCounter(NETWORK, NUM_WRITING_CONNECTIONS);
    }

    public static Counter numReadingConnections() {
        return MetricUtils.getCounter(NETWORK, NUM_READING_CONNECTIONS);
    }

    public static Counter numWritingFlows() {
        return MetricUtils.getCounter(NETWORK, NUM_WRITING_FLOWS);
    }

    public static Counter numReadingFlows() {
        return MetricUtils.getCounter(NETWORK, NUM_READING_FLOWS);
    }

    public static Meter numBytesWritingThroughput() {
        return MetricUtils.getMeter(NETWORK, NUM_BYTES_WRITING_THROUGHPUT);
    }

    public static Meter numBytesReadingThroughput() {
        return MetricUtils.getMeter(NETWORK, NUM_BYTES_READING_THROUGHPUT);
    }
}
