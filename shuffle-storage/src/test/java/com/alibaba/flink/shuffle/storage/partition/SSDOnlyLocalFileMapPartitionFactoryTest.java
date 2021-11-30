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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Tests for {@link SSDOnlyLocalFileMapPartitionFactory}. */
public class SSDOnlyLocalFileMapPartitionFactoryTest {

    @Rule public final TemporaryFolder temporaryFolder1 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder2 = new TemporaryFolder();

    @Test(expected = ConfigurationException.class)
    public void testPreferHddWithoutValidHddDataDir() {
        SSDOnlyLocalFileMapPartitionFactory partitionFactory =
                new SSDOnlyLocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                "[HDD]" + temporaryFolder1.getRoot().getAbsolutePath());
        partitionFactory.initialize(new Configuration(properties));
    }

    @Test
    public void testSSDOnly() {
        SSDOnlyLocalFileMapPartitionFactory partitionFactory =
                new SSDOnlyLocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format(
                        "[SSD]%s,[HDD]%s",
                        temporaryFolder1.getRoot().getAbsolutePath(),
                        temporaryFolder2.getRoot().getAbsolutePath()));
        partitionFactory.initialize(new Configuration(properties));

        for (int i = 0; i < 100; ++i) {
            StorageMeta storageMeta = partitionFactory.getNextDataStorageMeta();
            assertEquals(
                    StorageTestUtils.getStoragePath(temporaryFolder1),
                    storageMeta.getStoragePath());
            assertEquals(StorageType.SSD, storageMeta.getStorageType());
        }
    }
}
