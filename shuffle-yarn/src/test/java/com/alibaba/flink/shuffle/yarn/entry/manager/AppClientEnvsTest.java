/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

package com.alibaba.flink.shuffle.yarn.entry.manager;

import org.apache.commons.cli.ParseException;
import org.junit.Test;

import java.io.IOException;

import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_MEMORY_SIZE_KEY;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB;
import static org.junit.Assert.assertEquals;

/** Unit tests for {@link AppClientEnvs}. */
public class AppClientEnvsTest {
    @Test
    public void testShuffleManagerArgs() throws IOException, ParseException {
        String args =
                "-D remote-shuffle.yarn.manager-home-dir=aa "
                        + "-D ab1=cd1 "
                        + "-D ab2=cd2 "
                        + "-D "
                        + MANAGER_AM_MEMORY_SIZE_KEY
                        + "=10 "
                        + "-D "
                        + MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY
                        + "=10";
        String[] splitArgs = args.split(" ");
        AppClientEnvs envs =
                new AppClientEnvs(new org.apache.hadoop.conf.Configuration(), splitArgs);
        String shuffleManagerArgString = envs.getShuffleManagerArgs();

        assertContainsKeyVal(shuffleManagerArgString, "remote-shuffle.yarn.manager-home-dir", "aa");
        assertContainsKeyVal(shuffleManagerArgString, "ab1", "cd1");
        assertContainsKeyVal(shuffleManagerArgString, "ab2", "cd2");
        assertContainsKeyVal(
                shuffleManagerArgString,
                MANAGER_AM_MEMORY_SIZE_KEY,
                String.valueOf(MIN_VALID_AM_MEMORY_SIZE_MB));
        assertContainsKeyVal(
                shuffleManagerArgString,
                MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY,
                String.valueOf(MIN_VALID_AM_MEMORY_SIZE_MB));
        assertEquals(6, shuffleManagerArgString.split("=").length);
    }

    private static void assertContainsKeyVal(String argString, String key, String val) {
        assertEquals(val, argString.split("-D " + key + "=")[1].split(" ")[0]);
    }
}
