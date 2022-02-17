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

package com.alibaba.flink.shuffle.minicluster;

import com.alibaba.flink.shuffle.client.ShuffleManagerClient;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Tests the remote shuffle mini-cluster. */
public class ShuffleMiniClusterTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    private static final String partitionFactoryName =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory";

    @Test
    public void testMiniClusterWithDedicatedRpcService() throws Exception {
        ShuffleMiniClusterConfiguration configuration =
                new ShuffleMiniClusterConfiguration.Builder()
                        .setNumShuffleWorkers(2)
                        .setConfiguration(initConfiguration())
                        .build();

        try (ShuffleMiniCluster shuffleMiniCluster = new ShuffleMiniCluster(configuration)) {
            shuffleMiniCluster.start();

            JobID jobId = RandomIDUtils.randomJobId();
            ShuffleManagerClient client = shuffleMiniCluster.createClient(jobId);

            DataSetID dataSetId = RandomIDUtils.randomDataSetId();
            MapPartitionID mapPartitionId = RandomIDUtils.randomMapPartitionId();
            CompletableFuture<ShuffleResource> shuffleResource =
                    client.requestShuffleResource(
                            dataSetId, mapPartitionId, 2, partitionFactoryName);
            shuffleResource.get(60_000, TimeUnit.MILLISECONDS);
        }
    }

    private Configuration initConfiguration() throws IOException {
        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                TEMP_FOLDER.newFolder().getAbsolutePath());
        String address = InetAddress.getLocalHost().getHostAddress();
        configuration.setString(ManagerOptions.RPC_ADDRESS, address);
        configuration.setString(ManagerOptions.RPC_BIND_ADDRESS, address);
        return configuration;
    }
}
