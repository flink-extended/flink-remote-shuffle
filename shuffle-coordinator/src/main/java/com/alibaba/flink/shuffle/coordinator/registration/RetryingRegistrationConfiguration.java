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

package com.alibaba.flink.shuffle.coordinator.registration;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;

/** Configuration for the cluster components. */
public class RetryingRegistrationConfiguration {

    private final long errorDelayMillis;

    private final long refusedDelayMillis;

    public RetryingRegistrationConfiguration(long errorDelayMillis, long refusedDelayMillis) {
        checkArgument(errorDelayMillis >= 0, "delay on error must be non-negative");
        checkArgument(
                refusedDelayMillis >= 0, "delay on refused registration must be non-negative");

        this.errorDelayMillis = errorDelayMillis;
        this.refusedDelayMillis = refusedDelayMillis;
    }

    public long getErrorDelayMillis() {
        return errorDelayMillis;
    }

    public long getRefusedDelayMillis() {
        return refusedDelayMillis;
    }

    public static RetryingRegistrationConfiguration fromConfiguration(
            final Configuration configuration) {
        long errorDelayMillis =
                configuration.getDuration(ClusterOptions.ERROR_REGISTRATION_DELAY).toMillis();
        long refusedDelayMillis =
                configuration.getDuration(ClusterOptions.REFUSED_REGISTRATION_DELAY).toMillis();

        return new RetryingRegistrationConfiguration(errorDelayMillis, refusedDelayMillis);
    }
}
