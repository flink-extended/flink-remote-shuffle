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

package com.alibaba.flink.shuffle.coordinator.metrics;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.coordinator.manager.entrypoint.ShuffleManagerEntrypoint;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.RestOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.TransferOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for metrics. */
public class MetricsTest {

    private final Random random = new Random();

    private final int maxRetries = 10;

    private CloseableHttpClient httpClient;

    @Before
    public void before() throws Exception {
        httpClient = HttpClients.createDefault();
    }

    @After
    public void after() throws Exception {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Test
    public void testGetManagerMetrics() throws Exception {
        try (ShuffleManagerEntrypoint manager = createShuffleManager()) {
            manager.start();
            Configuration configuration = manager.getConfiguration();
            int port = configuration.getInteger(RestOptions.REST_MANAGER_BIND_PORT);
            HttpGet request = new HttpGet("http://127.0.0.1:" + port + "/metrics");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
                String metricsJson = EntityUtils.toString(response.getEntity());
                assertTrue(metricsJson.contains("\"jvm\":"));
                assertTrue(metricsJson.contains("\"system\":"));
                assertTrue(metricsJson.contains("remote-shuffle.cluster.num_jobs_serving"));
                assertTrue(metricsJson.contains("remote-shuffle.cluster.num_shuffle_workers"));
                assertTrue(
                        metricsJson.contains("remote-shuffle.cluster.total_num_data_partitions"));
                assertTrue(
                        metricsJson.contains("remote-shuffle.cluster.resource_request_throughput"));
                assertTrue(metricsJson.contains("remote-shuffle.cluster.hdd_max_free_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.cluster.ssd_max_free_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.cluster.hdd_max_used_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.cluster.ssd_max_used_bytes"));
            }
        }
    }

    @Test
    public void testGetWorkerMetrics() throws Exception {
        try (ShuffleWorkerRunner worker = createShuffleWorker()) {
            Configuration configuration = worker.getConfiguration();
            int port = configuration.getInteger(RestOptions.REST_WORKER_BIND_PORT);
            HttpGet request = new HttpGet("http://127.0.0.1:" + port + "/metrics");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
                String metricsJson = EntityUtils.toString(response.getEntity());
                assertTrue(metricsJson.contains("\"jvm\":"));
                assertTrue(metricsJson.contains("\"system\":"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.avg_data_file_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.avg_index_file_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.hdd_max_free_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.hdd_max_free_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.max_data_file_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.max_index_file_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.max_num_data_regions"));
                assertTrue(
                        metricsJson.contains(
                                "remote-shuffle.storage.num_available_reading_buffers"));
                assertTrue(
                        metricsJson.contains(
                                "remote-shuffle.storage.num_available_writing_buffers"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.num_data_partitions"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.total_num_executors"));
                assertTrue(
                        metricsJson.contains("remote-shuffle.storage.total_num_reading_buffers"));
                assertTrue(
                        metricsJson.contains("remote-shuffle.storage.total_num_writing_buffers"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.ssd_max_free_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.ssd_max_used_bytes"));
                assertTrue(
                        metricsJson.contains(
                                "remote-shuffle.storage.time_waiting_reading_buffers"));
                assertTrue(
                        metricsJson.contains(
                                "remote-shuffle.storage.time_waiting_writing_buffers"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.total_data_file_bytes"));
                assertTrue(metricsJson.contains("remote-shuffle.storage.total_index_file_bytes"));
                assertTrue(
                        metricsJson.contains("remote-shuffle.storage.total_partition_file_bytes"));
            }
        }
    }

    @Test
    public void testGetGroupMetrics() throws Exception {
        try (ShuffleWorkerRunner worker = createShuffleWorker()) {
            Configuration configuration = worker.getConfiguration();
            int port = configuration.getInteger(RestOptions.REST_WORKER_BIND_PORT);
            HttpGet request =
                    new HttpGet("http://127.0.0.1:" + port + "/metrics?group=remote-shuffle");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
                String metricsJson = EntityUtils.toString(response.getEntity());
                assertFalse(metricsJson.contains("\"jvm\":"));
                assertFalse(metricsJson.contains("\"system\":"));
                assertTrue(metricsJson.contains("remote-shuffle"));
            }
        }
    }

    @Test
    public void testGetMultiGroupMetrics() throws Exception {
        try (ShuffleWorkerRunner worker = createShuffleWorker()) {
            Configuration configuration = worker.getConfiguration();
            int port = configuration.getInteger(RestOptions.REST_WORKER_BIND_PORT);
            HttpGet request =
                    new HttpGet("http://127.0.0.1:" + port + "/metrics?group=jvm&group=system");

            try (CloseableHttpResponse response = httpClient.execute(request)) {
                assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
                String metricsJson = EntityUtils.toString(response.getEntity());
                assertTrue(metricsJson.contains("\"jvm\":"));
                assertTrue(metricsJson.contains("\"system\":"));
                assertFalse(metricsJson.contains("remote-shuffle"));
            }
        }
    }

    private ShuffleManagerEntrypoint createShuffleManager() throws Exception {
        Configuration configuration = new Configuration();
        for (int retry = 0; retry < maxRetries; ++retry) {
            try {
                configuration.setInteger(ManagerOptions.RPC_PORT, randomPort());
                configuration.setInteger(RestOptions.REST_MANAGER_BIND_PORT, randomPort());
                return new ShuffleManagerEntrypoint(configuration);
            } catch (Throwable throwable) {
                if (retry == maxRetries - 1) {
                    throw throwable;
                }
            }
        }
        throw new RuntimeException("Should never reach here.");
    }

    private ShuffleWorkerRunner createShuffleWorker() throws Exception {
        Configuration configuration = new Configuration();
        for (int retry = 0; retry < maxRetries; ++retry) {
            try {
                configuration.setMemorySize(
                        MemoryOptions.MEMORY_SIZE_FOR_DATA_READING, MemorySize.parse("128m"));
                configuration.setMemorySize(
                        MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING, MemorySize.parse("128m"));
                configuration.setString(StorageOptions.STORAGE_LOCAL_DATA_DIRS, "/tmp");
                configuration.setString(ManagerOptions.RPC_ADDRESS, "127.0.0.1");
                configuration.setString(WorkerOptions.HOST, "127.0.0.1");
                configuration.setInteger(TransferOptions.SERVER_DATA_PORT, randomPort());
                configuration.setInteger(RestOptions.REST_WORKER_BIND_PORT, randomPort());
                return new ShuffleWorkerRunner(configuration);
            } catch (Throwable throwable) {
                if (retry == maxRetries - 1) {
                    throw throwable;
                }
            }
        }
        throw new RuntimeException("Should never reach here.");
    }

    private int randomPort() {
        return random.nextInt(40000) + 20000;
    }
}
