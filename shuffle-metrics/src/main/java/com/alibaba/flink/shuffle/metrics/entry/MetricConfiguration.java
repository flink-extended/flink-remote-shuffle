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

package com.alibaba.flink.shuffle.metrics.entry;

import com.alibaba.flink.shuffle.common.config.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.core.config.MetricOptions.METRICS_BIND_HOST;
import static com.alibaba.flink.shuffle.core.config.MetricOptions.METRICS_HTTP_SERVER_ENABLE;
import static com.alibaba.flink.shuffle.core.config.MetricOptions.METRICS_SHUFFLE_MANAGER_HTTP_BIND_PORT;
import static com.alibaba.flink.shuffle.core.config.MetricOptions.METRICS_SHUFFLE_WORKER_HTTP_BIND_PORT;

/**
 * This class is used to transform configurations, because the configurations can't be used directly
 * in the dependency metrics project.
 */
public class MetricConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(MetricConfiguration.class);
    // Config key used in the dependency metrics project.
    private static final String ALI_METRICS_BINDING_HOST = "com.alibaba.metrics.http.binding.host";
    private static final String ALI_METRICS_HTTP_PORT = "com.alibaba.metrics.http.port";

    private final Configuration conf;

    private final Properties properties;

    private final boolean isManager;

    public MetricConfiguration(Configuration configuration, boolean isManager) {
        this.conf = configuration;
        this.isManager = isManager;
        this.properties = parseMetricProperties(configuration);
    }

    /** Transform configurations into new formats used in the dependency metrics project. */
    private Properties parseMetricProperties(Configuration configuration) {
        checkNotNull(configuration);
        Properties properties = new Properties();

        // Transfer the bind host config from cluster config
        String bindHost = configuration.getString(METRICS_BIND_HOST);
        System.setProperty(ALI_METRICS_BINDING_HOST, bindHost);

        int bindPort;
        if (isManager) {
            bindPort = configuration.getInteger(METRICS_SHUFFLE_MANAGER_HTTP_BIND_PORT);
        } else {
            bindPort = configuration.getInteger(METRICS_SHUFFLE_WORKER_HTTP_BIND_PORT);
        }
        System.setProperty(ALI_METRICS_HTTP_PORT, Integer.toString(bindPort));
        LOG.info("Metrics http server port is set to " + bindPort);

        properties.putAll(configuration.toProperties());
        return properties;
    }

    boolean isHttpServerEnabled() {
        return conf == null ? false : conf.getBoolean(METRICS_HTTP_SERVER_ENABLE);
    }

    public Properties getProperties() {
        return properties;
    }

    public Configuration getConfiguration() {
        return conf;
    }
}
