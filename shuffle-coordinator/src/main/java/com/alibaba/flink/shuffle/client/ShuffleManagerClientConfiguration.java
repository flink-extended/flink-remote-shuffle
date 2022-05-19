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

package com.alibaba.flink.shuffle.client;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.registration.RetryingRegistrationConfiguration;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.RpcOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** The configuration of shuffle manager client. */
public class ShuffleManagerClientConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(ShuffleManagerClientConfiguration.class);

    private final Configuration configuration;

    private final long rpcTimeout;

    private final long maxRegistrationDuration;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    public ShuffleManagerClientConfiguration(
            Configuration configuration,
            long rpcTimeout,
            long maxRegistrationDuration,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {

        this.configuration = configuration;
        this.rpcTimeout = rpcTimeout;
        this.maxRegistrationDuration = maxRegistrationDuration;
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public long getRpcTimeout() {
        return rpcTimeout;
    }

    public long getMaxRegistrationDuration() {
        return maxRegistrationDuration;
    }

    public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
        return retryingRegistrationConfiguration;
    }

    // --------------------------------------------------------------------------------------------
    //  Static factory methods
    // --------------------------------------------------------------------------------------------

    public static ShuffleManagerClientConfiguration fromConfiguration(Configuration configuration) {
        long rpcTimeout;
        try {
            rpcTimeout = configuration.getDuration(RpcOptions.RPC_TIMEOUT).toMillis();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid format for '"
                            + RpcOptions.RPC_TIMEOUT.key()
                            + "'.Use formats like '50 s' or '1 min' to specify the timeout.");
        }
        LOG.debug("Messages have a max timeout of {}.", rpcTimeout);

        long maxRegistrationDuration;
        try {
            maxRegistrationDuration =
                    configuration.getDuration(ClusterOptions.REGISTRATION_TIMEOUT).toMillis();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid format for parameter %s. Set the timeout to be infinite.",
                            ClusterOptions.REGISTRATION_TIMEOUT.key()),
                    e);
        }

        RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);
        return new ShuffleManagerClientConfiguration(
                configuration,
                rpcTimeout,
                maxRegistrationDuration,
                retryingRegistrationConfiguration);
    }
}
