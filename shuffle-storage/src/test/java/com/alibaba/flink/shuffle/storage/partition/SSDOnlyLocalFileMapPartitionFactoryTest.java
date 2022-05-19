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

import static com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactoryTest.assertExpectedStorageMeta;
import static com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactoryTest.expectedEvenDiskUsedCount;
import static com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactoryTest.updateStorageFreeSpace;
import static com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactoryTest.updateStorageHealthStatus;
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

    @Test
    public void testUpdateFreeStorageSpace() {
        LocalFileMapPartitionFactory partitionFactory = new SSDOnlyLocalFileMapPartitionFactory();
        LocalFileMapPartitionFactoryTest.FakeStorageMeta[] storageMetas =
                LocalFileMapPartitionFactoryTest.addStorageMetas(partitionFactory);

        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());

        LocalFileMapPartitionFactoryTest.updateStorageFreeSpace(storageMetas, 4, partitionFactory);
        assertEquals(7, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        LocalFileMapPartitionFactoryTest.updateStorageFreeSpace(storageMetas, 0, partitionFactory);
        assertEquals(3, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
    }

    @Test
    public void testUpdateFreeStorageSpaceWithUnhealthyStorage() {
        LocalFileMapPartitionFactory partitionFactory = new SSDOnlyLocalFileMapPartitionFactory();
        LocalFileMapPartitionFactoryTest.FakeStorageMeta[] storageMetas =
                LocalFileMapPartitionFactoryTest.addStorageMetas(partitionFactory);
        updateStorageFreeSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertEquals(1, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageHealthStatus(storageMetas, 0, 2, false, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertEquals(3, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
    }

    @Test
    public void testGetNextStorageMetaWithUnhealthyStorage() {
        LocalFileMapPartitionFactory partitionFactory = new SSDOnlyLocalFileMapPartitionFactory();
        LocalFileMapPartitionFactoryTest.FakeStorageMeta[] storageMetas =
                LocalFileMapPartitionFactoryTest.addStorageMetas(partitionFactory);
        updateStorageFreeSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[0]);

        updateStorageHealthStatus(storageMetas, 0, 2, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, null);

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[2]);
    }

    @Test
    public void testAllDisksWillBeUsed() {
        SSDOnlyLocalFileMapPartitionFactory partitionFactory =
                new SSDOnlyLocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format(
                        "[SSD]%s,[SSD]%s",
                        temporaryFolder1.getRoot().getAbsolutePath(),
                        temporaryFolder2.getRoot().getAbsolutePath()));
        partitionFactory.initialize(new Configuration(properties));

        expectedEvenDiskUsedCount(
                partitionFactory, temporaryFolder1, temporaryFolder2, StorageType.SSD);
    }
}
