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

import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Tests for the {@link RetryingRegistrationConfiguration}. */
public class RetryingRegistrationConfigurationTest {

    @Test
    public void testConfigurationParsing() {
        final Configuration configuration = new Configuration();
        final Duration refusedRegistrationDelay = Duration.ofMillis(3);
        final Duration errorRegistrationDelay = Duration.ofMillis(4);

        configuration.setDuration(
                ClusterOptions.REFUSED_REGISTRATION_DELAY, refusedRegistrationDelay);
        configuration.setDuration(ClusterOptions.ERROR_REGISTRATION_DELAY, errorRegistrationDelay);

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);
        assertThat(
                retryingRegistrationConfiguration.getRefusedDelayMillis(),
                is(refusedRegistrationDelay.toMillis()));
        assertThat(
                retryingRegistrationConfiguration.getErrorDelayMillis(),
                is(errorRegistrationDelay.toMillis()));
    }
}
