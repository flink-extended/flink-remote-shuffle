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

import static com.alibaba.flink.shuffle.storage.partition.LocalFileReducePartitionFactoryTest.assertExpectedStorageMeta;
import static com.alibaba.flink.shuffle.storage.partition.LocalFileReducePartitionFactoryTest.expectedReducePartitionEvenDiskUsedCount;
import static com.alibaba.flink.shuffle.storage.partition.LocalFileReducePartitionFactoryTest.updateStorageFreeSpace;
import static com.alibaba.flink.shuffle.storage.partition.LocalFileReducePartitionFactoryTest.updateStorageHealthStatus;
import static org.junit.Assert.assertEquals;

/** Tests for {@link HDDOnlyLocalFileReducePartitionFactory}. */
public class HDDOnlyLocalFileReducePartitionFactoryTest {

    @Rule public final TemporaryFolder temporaryFolder1 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder2 = new TemporaryFolder();

    @Test(expected = ConfigurationException.class)
    public void testWithoutValidHddDataDir() {
        HDDOnlyLocalFileReducePartitionFactory partitionFactory =
                new HDDOnlyLocalFileReducePartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                "[SSD]" + temporaryFolder1.getRoot().getAbsolutePath());
        partitionFactory.initialize(new Configuration(properties));
    }

    @Test
    public void testHDDOnly() {
        HDDOnlyLocalFileReducePartitionFactory partitionFactory =
                new HDDOnlyLocalFileReducePartitionFactory();
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
                    StorageTestUtils.getStoragePath(temporaryFolder2),
                    storageMeta.getStoragePath());
            assertEquals(StorageType.HDD, storageMeta.getStorageType());
        }
    }

    @Test
    public void testUpdateFreeStorageSpace() {
        LocalFileReducePartitionFactory partitionFactory =
                new HDDOnlyLocalFileReducePartitionFactory();
        LocalFileReducePartitionFactoryTest.FakeStorageMeta[] storageMetas =
                LocalFileReducePartitionFactoryTest.addStorageMetas(partitionFactory);

        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());

        updateStorageFreeSpace(storageMetas, 4, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(8, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageFreeSpace(storageMetas, 0, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(4, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
    }

    @Test
    public void testUpdateFreeStorageSpaceWithUnhealthyStorage() {
        LocalFileReducePartitionFactory partitionFactory =
                new HDDOnlyLocalFileReducePartitionFactory();
        LocalFileReducePartitionFactoryTest.FakeStorageMeta[] storageMetas =
                LocalFileReducePartitionFactoryTest.addStorageMetas(partitionFactory);
        updateStorageFreeSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(2, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageHealthStatus(storageMetas, 0, 2, false, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(4, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
    }

    @Test
    public void testGetNextStorageMetaWithUnhealthyStorage() {
        LocalFileReducePartitionFactory partitionFactory =
                new HDDOnlyLocalFileReducePartitionFactory();
        LocalFileReducePartitionFactoryTest.FakeStorageMeta[] storageMetas =
                LocalFileReducePartitionFactoryTest.addStorageMetas(partitionFactory);
        updateStorageFreeSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[1]);

        updateStorageHealthStatus(storageMetas, 0, 2, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, null);

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[3]);
    }

    @Test
    public void testAllDisksWillBeUsed() {
        HDDOnlyLocalFileReducePartitionFactory partitionFactory =
                new HDDOnlyLocalFileReducePartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format(
                        "[HDD]%s,[HDD]%s",
                        temporaryFolder1.getRoot().getAbsolutePath(),
                        temporaryFolder2.getRoot().getAbsolutePath()));
        partitionFactory.initialize(new Configuration(properties));

        expectedReducePartitionEvenDiskUsedCount(
                partitionFactory, temporaryFolder1, temporaryFolder2, StorageType.HDD);
    }
}