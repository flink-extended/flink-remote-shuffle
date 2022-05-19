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

package com.alibaba.flink.shuffle.rest;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.RestOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utils to start rest service. */
public class RestUtil {

    private static final Logger LOG = LoggerFactory.getLogger(RestUtil.class);

    public static RestService startManagerRestService(Configuration configuration)
            throws Exception {
        try {
            String host = configuration.getString(RestOptions.REST_BIND_HOST);
            int port = configuration.getInteger(RestOptions.REST_MANAGER_BIND_PORT);
            RestService restService = new RestService(host, port);
            restService.start();
            return restService;
        } catch (Throwable throwable) {
            LOG.error("Failed to start rest service.", throwable);
            throw throwable;
        }
    }

    public static RestService startWorkerRestService(Configuration configuration) throws Exception {
        try {
            String host = configuration.getString(RestOptions.REST_BIND_HOST);
            int port = configuration.getInteger(RestOptions.REST_WORKER_BIND_PORT);
            RestService restService = new RestService(host, port);
            restService.start();
            return restService;
        } catch (Throwable throwable) {
            LOG.error("Failed to start rest service.", throwable);
            throw throwable;
        }
    }
}
