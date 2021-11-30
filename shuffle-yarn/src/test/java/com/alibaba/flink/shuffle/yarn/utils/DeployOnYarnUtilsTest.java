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

package com.alibaba.flink.shuffle.yarn.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Unit Tests for {@link DeployOnYarnUtils}. */
public class DeployOnYarnUtilsTest {

    @Test
    public void testHadoopConfToMaps() {
        YarnConfiguration hadoopConf = new YarnConfiguration();
        String testKey = "a.b.test.key";
        String testVal = "a.b.test.val";
        hadoopConf.set(testKey, testVal);
        Map<String, String> confMaps = DeployOnYarnUtils.hadoopConfToMaps(hadoopConf);
        assertEquals(confMaps.get(testKey), testVal);
    }

    @Test
    public void testParseParameters() throws ParseException {
        String[] inputArgs =
                "-D a.k1=a.v1 -Da.k2=a.v2 -D remote-shuffle.yarn.worker-stop-on-failure=true"
                        .split(" ");
        Configuration conf = DeployOnYarnUtils.parseParameters(inputArgs);
        assertEquals(conf.getString("a.k1"), "a.v1");
        assertEquals(conf.getString("a.k2"), "a.v2");
        assertEquals(conf.getBoolean(YarnOptions.WORKER_STOP_ON_FAILURE), true);
    }
}
