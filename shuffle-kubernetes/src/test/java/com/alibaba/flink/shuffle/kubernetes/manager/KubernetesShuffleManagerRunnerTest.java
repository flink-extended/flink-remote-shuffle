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

package com.alibaba.flink.shuffle.kubernetes.manager;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.kubernetes.manager.util.EnvUtils;

import org.junit.Test;

import java.util.Collections;

import static com.alibaba.flink.shuffle.kubernetes.manager.KubernetesShuffleManagerRunner.ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Test for {@link KubernetesShuffleManagerRunner}. */
public class KubernetesShuffleManagerRunnerTest {

    @Test
    public void testLoadConfigurationWithNoneHaMode() {
        EnvUtils.setEnv(Collections.singletonMap(ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS, "192.168.1.1"));
        Configuration configuration = new Configuration();
        configuration.setString(ManagerOptions.RPC_ADDRESS, "localhost");
        configuration.setString(HighAvailabilityOptions.HA_MODE, "NONE");

        Configuration updatedConfiguration =
                KubernetesShuffleManagerRunner.loadConfiguration(configuration);
        assertThat(updatedConfiguration.getString(ManagerOptions.RPC_ADDRESS), is("localhost"));
    }

    @Test
    public void testLoadConfigurationWithZooKeeperHaMode() {
        EnvUtils.setEnv(Collections.singletonMap(ENV_REMOTE_SHUFFLE_POD_IP_ADDRESS, "192.168.1.1"));
        Configuration configuration = new Configuration();
        configuration.setString(ManagerOptions.RPC_ADDRESS, "localhost");
        configuration.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");

        Configuration updatedConfiguration =
                KubernetesShuffleManagerRunner.loadConfiguration(configuration);
        assertThat(updatedConfiguration.getString(ManagerOptions.RPC_ADDRESS), is("192.168.1.1"));
    }
}
