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

package com.alibaba.flink.shuffle.kubernetes.operator.crd;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** Test for {@link RemoteShuffleApplicationSpec}. */
public class RemoteShuffleApplicationSpecTest {

    private final Map<String, String> dynamicConfigs = new HashMap<>();

    private final Map<String, String> fileConfigs = new HashMap<>();

    @Before
    public void sutUp() {
        dynamicConfigs.put("key1", "value1");
        dynamicConfigs.put("key2", "value2");
        dynamicConfigs.put("key3", "value3");

        fileConfigs.put("file1", "This is a test for config file.");
        fileConfigs.put("file2", "This is a test for config file.");
    }

    @Test
    public void testToString() {
        RemoteShuffleApplicationSpec spec =
                new RemoteShuffleApplicationSpec(dynamicConfigs, fileConfigs);
        assertEquals(
                spec.toString(),
                "RemoteShuffleApplicationSpec("
                        + "shuffleDynamicConfigs={key1=value1, key2=value2, key3=value3}, "
                        + "shuffleFileConfigs={file2=This is a test for config file., "
                        + "file1=This is a test for config file.})");
    }

    @Test
    public void testEquals() {
        RemoteShuffleApplicationSpec spec1 =
                new RemoteShuffleApplicationSpec(dynamicConfigs, fileConfigs);
        RemoteShuffleApplicationSpec spec2 =
                new RemoteShuffleApplicationSpec(dynamicConfigs, fileConfigs);
        RemoteShuffleApplicationSpec spec3 =
                new RemoteShuffleApplicationSpec(Collections.emptyMap(), fileConfigs);

        assertEquals(spec1, spec2);
        assertNotEquals(spec1, spec3);
    }
}
