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

package com.alibaba.flink.shuffle.e2e.shufflecluster;

import com.alibaba.flink.shuffle.common.config.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Utils for {@link LocalShuffleCluster}. */
public class LocalShuffleClusterUtils {
    static String[] generateDynamicConfigs(Configuration configuration) {
        List<String> dynamicConfigs = new ArrayList<>();
        configuration.toMap().entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(
                        kv -> {
                            dynamicConfigs.add("-D");
                            dynamicConfigs.add(String.format("%s=%s", kv.getKey(), kv.getValue()));
                        });

        return dynamicConfigs.toArray(new String[dynamicConfigs.size()]);
    }
}
