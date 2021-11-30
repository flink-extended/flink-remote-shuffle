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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.Executor;

import static org.junit.Assert.assertSame;

/** Tests for the {@link HaServiceUtils} class. */
public class HighAvailabilityServicesUtilsTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testCreateCustomHAServices() throws Exception {
        Configuration config = new Configuration();

        HaServices haServices = new TestingHighAvailabilityServices();
        TestHAFactory.haServices = haServices;

        Executor executor = Runnable::run;

        config.setString(HighAvailabilityOptions.HA_MODE, TestHAFactory.class.getName());

        // when
        HaServices actualHaServices =
                HaServiceUtils.createAvailableOrEmbeddedServices(config, executor);

        // then
        assertSame(haServices, actualHaServices);

        // when
        actualHaServices = HaServiceUtils.createHAServices(config);
        // then
        assertSame(haServices, actualHaServices);
    }

    @Test(expected = Exception.class)
    public void testCustomHAServicesFactoryNotDefined() throws Exception {
        Configuration config = new Configuration();

        Executor executor = Runnable::run;

        config.setString(
                HighAvailabilityOptions.HA_MODE, HaMode.FACTORY_CLASS.name().toLowerCase());

        // expect
        HaServiceUtils.createAvailableOrEmbeddedServices(config, executor);
    }

    /** Testing class which needs to be public in order to be instantiatable. */
    public static class TestHAFactory implements HaServicesFactory {

        static HaServices haServices;

        @Override
        public HaServices createHAServices(Configuration configuration) {
            return haServices;
        }
    }
}
