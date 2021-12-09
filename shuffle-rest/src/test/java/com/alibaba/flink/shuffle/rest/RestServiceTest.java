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

package com.alibaba.flink.shuffle.rest;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletResponse;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** Tests for {@link RestService}. */
public class RestServiceTest {

    private static final String resultJson = "{\"result\": \"success\"}";

    private RestService restService;

    private CloseableHttpClient httpClient;

    private int port;

    @Before
    public void before() throws Exception {
        Random random = new Random();
        int maxRetries = 10;
        for (int retry = 0; retry < maxRetries; ++retry) {
            try {
                port = random.nextInt(40000) + 20000;
                restService = new RestService("127.0.0.1", port);
                restService.start();
                restService.registerHandler(new TestHandler());
                break;
            } catch (Throwable throwable) {
                if (restService != null) {
                    CommonUtils.runQuietly(restService::stop);
                    restService = null;
                }
                if (retry == maxRetries - 1) {
                    throw throwable;
                }
            }
        }

        httpClient = HttpClients.createDefault();
    }

    @After
    public void after() throws Exception {
        if (restService != null) {
            restService.stop();
        }

        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Test
    public void testRequestSuccess() throws Exception {
        HttpGet request = new HttpGet("http://127.0.0.1:" + port + "/test");
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            assertEquals(HttpServletResponse.SC_OK, response.getStatusLine().getStatusCode());
            assertEquals(resultJson, EntityUtils.toString(response.getEntity()));
        }
    }

    @Test
    public void testPageNotFound() throws Exception {
        HttpGet request = new HttpGet("http://127.0.0.1:" + port + "/nonexistent");
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            assertEquals(
                    HttpServletResponse.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void testRequestProcessFailure() throws Exception {
        HttpGet request = new HttpGet("http://127.0.0.1:" + port + "/test?key=value");
        try (CloseableHttpResponse response = httpClient.execute(request)) {
            assertEquals(
                    HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    response.getStatusLine().getStatusCode());
        }
    }

    private static class TestHandler implements RestHandler {

        @Override
        public RestResponse handle(Map<String, String[]> parameters) {
            if (parameters.isEmpty()) {
                return () -> resultJson;
            } else {
                throw new IllegalArgumentException("Unknown parameters.");
            }
        }

        @Override
        public String getQueryPath() {
            return "/test";
        }
    }
}
