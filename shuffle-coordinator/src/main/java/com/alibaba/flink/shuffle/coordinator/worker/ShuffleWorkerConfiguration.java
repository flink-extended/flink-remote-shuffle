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

package com.alibaba.flink.shuffle.coordinator.worker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.registration.RetryingRegistrationConfiguration;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;

/** The parsed configuration values for {@link ShuffleWorker}. */
public class ShuffleWorkerConfiguration {

    private final Configuration configuration;

    private final long maxRegistrationDuration;

    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    public ShuffleWorkerConfiguration(
            Configuration configuration,
            long maxRegistrationDuration,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration) {
        CommonUtils.checkArgument(configuration != null, "Must be not null.");
        CommonUtils.checkArgument(retryingRegistrationConfiguration != null, "Must be not null.");

        this.configuration = configuration;
        this.maxRegistrationDuration = maxRegistrationDuration;
        this.retryingRegistrationConfiguration = retryingRegistrationConfiguration;
    }

    public static ShuffleWorkerConfiguration fromConfiguration(Configuration configuration) {
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

        return new ShuffleWorkerConfiguration(
                configuration, maxRegistrationDuration, retryingRegistrationConfiguration);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public long getMaxRegistrationDuration() {
        return maxRegistrationDuration;
    }

    // --------------------------------------------------------------------------------------------
    //  Static factory methods
    // --------------------------------------------------------------------------------------------

    public RetryingRegistrationConfiguration getRetryingRegistrationConfiguration() {
        return retryingRegistrationConfiguration;
    }
}
