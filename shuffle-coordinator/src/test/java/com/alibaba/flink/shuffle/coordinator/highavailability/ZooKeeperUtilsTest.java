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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperUtils;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.utils.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link ZooKeeperUtils}. */
public class ZooKeeperUtilsTest extends TestLogger {

    @Test
    public void testZooKeeperEnsembleConnectStringConfiguration() throws Exception {
        // ZooKeeper does not like whitespace in the quorum connect String.
        String actual, expected;
        Configuration conf = new Configuration();

        {
            expected = "localhost:2891";

            setQuorum(conf, expected);
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);

            setQuorum(conf, " localhost:2891 "); // with leading and trailing whitespace
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);

            setQuorum(conf, "localhost :2891"); // whitespace after port
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);
        }

        {
            expected = "localhost:2891,localhost:2891";

            setQuorum(conf, "localhost:2891,localhost:2891");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);

            setQuorum(conf, "localhost:2891, localhost:2891");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);

            setQuorum(conf, "localhost :2891, localhost:2891");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);

            setQuorum(conf, " localhost:2891, localhost:2891 ");
            actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
            assertEquals(expected, actual);
        }
    }

    private Configuration setQuorum(Configuration conf, String quorum) {
        conf.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, quorum);
        return conf;
    }
}
