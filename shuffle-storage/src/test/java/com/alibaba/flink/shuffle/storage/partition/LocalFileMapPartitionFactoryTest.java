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
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalFileMapPartitionFactory}. */
public class LocalFileMapPartitionFactoryTest {

    @Rule public final TemporaryFolder temporaryFolder1 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder2 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder3 = new TemporaryFolder();

    @Test(expected = ConfigurationException.class)
    public void testDataDirNotConfigured() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        partitionFactory.initialize(new Configuration(new Properties()));
    }

    @Test(expected = ConfigurationException.class)
    public void testIllegalDiskType() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(StorageOptions.STORAGE_PREFERRED_TYPE.key(), "Illegal");
        partitionFactory.initialize(new Configuration(properties));
    }

    @Test(expected = ConfigurationException.class)
    public void testConfiguredDataDirNotExists() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(), "Illegal");
        partitionFactory.initialize(new Configuration(properties));
    }

    @Test(expected = ConfigurationException.class)
    public void testConfiguredDataDirIsNotDirectory() throws IOException {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                temporaryFolder1.newFile().getAbsolutePath());
        partitionFactory.initialize(new Configuration(properties));
    }

    @Test(expected = ConfigurationException.class)
    public void testNoValidDataDir() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(), " ");
        partitionFactory.initialize(new Configuration(properties));
    }

    @Test
    public void testInitialization() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";
        String path3 = temporaryFolder3.getRoot().getAbsolutePath() + "/";

        StorageMeta storageMeta1 = new LocalFileStorageMeta(path1, StorageType.SSD);
        StorageMeta storageMeta2 = new LocalFileStorageMeta(path2, StorageType.HDD);
        StorageMeta storageMeta3 = new LocalFileStorageMeta(path3, StorageType.HDD);

        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format("[SSD]%s,[HDD]%s,%s", path1, path2, path3));
        properties.setProperty(StorageOptions.STORAGE_PREFERRED_TYPE.key(), "HDD");
        partitionFactory.initialize(new Configuration(properties));

        assertEquals(1, partitionFactory.getSsdStorageMetas().size());
        assertTrue(partitionFactory.getSsdStorageMetas().contains(storageMeta1));

        assertEquals(2, partitionFactory.getHddStorageMetas().size());
        assertTrue(partitionFactory.getHddStorageMetas().contains(storageMeta2));
        assertTrue(partitionFactory.getHddStorageMetas().contains(storageMeta3));

        assertEquals(StorageType.HDD, partitionFactory.getPreferredStorageType());
    }

    @Test
    public void testFairness() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";
        String path3 = temporaryFolder3.getRoot().getAbsolutePath() + "/";

        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format("[SSD]%s,[SSD]%s,[SSD]%s", path1, path2, path3));
        partitionFactory.initialize(new Configuration(properties));

        List<String> selectedDirs = new ArrayList<>();
        for (int i = 0; i < 30; ++i) {
            DataPartition dataPartition =
                    partitionFactory.createDataPartition(
                            StorageTestUtils.NO_OP_PARTITIONED_DATA_STORE,
                            StorageTestUtils.JOB_ID,
                            StorageTestUtils.DATA_SET_ID,
                            StorageTestUtils.MAP_PARTITION_ID,
                            StorageTestUtils.NUM_MAP_PARTITIONS,
                            StorageTestUtils.NUM_REDUCE_PARTITIONS);
            selectedDirs.add(dataPartition.getPartitionMeta().getStorageMeta().getStoragePath());
        }

        int path1Count = 0;
        int path2Count = 0;
        int path3Count = 0;
        for (String path : selectedDirs) {
            if (path.equals(path1)) {
                ++path1Count;
            } else if (path.equals(path2)) {
                ++path2Count;
            } else if (path.equals(path3)) {
                ++path3Count;
            }
        }

        assertEquals(10, path1Count);
        assertEquals(10, path2Count);
        assertEquals(10, path3Count);
    }

    @Test
    public void testPreferredDiskType() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";

        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format("[SSD]%s,[HDD]%s", path1, path2));
        properties.setProperty(StorageOptions.STORAGE_PREFERRED_TYPE.key(), "HDD");
        partitionFactory.initialize(new Configuration(properties));

        for (int i = 0; i < 100; ++i) {
            DataPartition dataPartition =
                    partitionFactory.createDataPartition(
                            StorageTestUtils.NO_OP_PARTITIONED_DATA_STORE,
                            StorageTestUtils.JOB_ID,
                            StorageTestUtils.DATA_SET_ID,
                            StorageTestUtils.MAP_PARTITION_ID,
                            StorageTestUtils.NUM_MAP_PARTITIONS,
                            StorageTestUtils.NUM_REDUCE_PARTITIONS);
            assertEquals(path2, dataPartition.getPartitionMeta().getStorageMeta().getStoragePath());
        }
    }

    @Test
    public void testUpdateUsableStorageSpace() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        FakeStorageMeta[] storageMetas = addStorageMetas(partitionFactory);

        assertEquals(0, partitionFactory.getUsableStorageSpace().getHddUsableSpaceBytes());
        assertEquals(0, partitionFactory.getUsableStorageSpace().getSsdUsableSpaceBytes());

        updateStorageUsableSpace(storageMetas, 4, partitionFactory);
        assertEquals(7, partitionFactory.getUsableStorageSpace().getSsdUsableSpaceBytes());
        assertEquals(8, partitionFactory.getUsableStorageSpace().getHddUsableSpaceBytes());

        updateStorageUsableSpace(storageMetas, 0, partitionFactory);
        assertEquals(3, partitionFactory.getUsableStorageSpace().getSsdUsableSpaceBytes());
        assertEquals(4, partitionFactory.getUsableStorageSpace().getHddUsableSpaceBytes());
    }

    @Test
    public void testUpdateUsableStorageSpaceWithUnhealthyStorage() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        FakeStorageMeta[] storageMetas = addStorageMetas(partitionFactory);
        updateStorageUsableSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertEquals(1, partitionFactory.getUsableStorageSpace().getSsdUsableSpaceBytes());
        assertEquals(2, partitionFactory.getUsableStorageSpace().getHddUsableSpaceBytes());

        updateStorageHealthStatus(storageMetas, 0, 2, false, partitionFactory);
        assertEquals(0, partitionFactory.getUsableStorageSpace().getSsdUsableSpaceBytes());
        assertEquals(0, partitionFactory.getUsableStorageSpace().getHddUsableSpaceBytes());

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertEquals(3, partitionFactory.getUsableStorageSpace().getSsdUsableSpaceBytes());
        assertEquals(4, partitionFactory.getUsableStorageSpace().getHddUsableSpaceBytes());
    }

    static FakeStorageMeta[] addStorageMetas(LocalFileMapPartitionFactory partitionFactory) {
        partitionFactory.preferredStorageType = StorageType.SSD;
        FakeStorageMeta ssdStorageMeta1 = new FakeStorageMeta("ssd1", StorageType.SSD);
        FakeStorageMeta hddStorageMeta1 = new FakeStorageMeta("hdd1", StorageType.HDD);
        FakeStorageMeta ssdStorageMeta2 = new FakeStorageMeta("ssd2", StorageType.SSD);
        FakeStorageMeta hddStorageMeta2 = new FakeStorageMeta("hdd2", StorageType.HDD);

        partitionFactory.addSsdStorageMeta(ssdStorageMeta1);
        partitionFactory.addHddStorageMeta(hddStorageMeta1);
        partitionFactory.addSsdStorageMeta(ssdStorageMeta2);
        partitionFactory.addHddStorageMeta(hddStorageMeta2);

        return new FakeStorageMeta[] {
            ssdStorageMeta1, hddStorageMeta1, ssdStorageMeta2, hddStorageMeta2
        };
    }

    static void updateStorageUsableSpace(
            FakeStorageMeta[] storageMetas,
            long base,
            LocalFileMapPartitionFactory partitionFactory) {
        for (int i = 1; i <= storageMetas.length; ++i) {
            storageMetas[i - 1].updateUsableStorageSpace(i + base);
        }
        partitionFactory.updateUsableStorageSpace();
    }

    static void updateStorageHealthStatus(
            FakeStorageMeta[] storageMetas,
            int startIndex,
            int endIndex,
            boolean isHealthy,
            LocalFileMapPartitionFactory partitionFactory) {
        for (int i = startIndex; i < endIndex; ++i) {
            storageMetas[i].updateStorageHealthStatus(isHealthy);
        }
        partitionFactory.updateUsableStorageSpace();
    }

    @Test
    public void testGetNextStorageMetaWithUnhealthyStorage() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        FakeStorageMeta[] storageMetas = addStorageMetas(partitionFactory);
        updateStorageUsableSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[0]);

        updateStorageHealthStatus(storageMetas, 0, 1, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[1]);

        updateStorageHealthStatus(storageMetas, 1, 2, false, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, null);

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertExpectedStorageMeta(partitionFactory, storageMetas[2]);
    }

    static void assertExpectedStorageMeta(
            LocalFileMapPartitionFactory partitionFactory, StorageMeta expectedStorageMeta) {
        for (int i = 0; i < 1024; ++i) {
            assertEquals(expectedStorageMeta, partitionFactory.getNextDataStorageMeta());
        }
    }

    @Test
    public void testAllDisksWillBeUsed() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format(
                        "[HDD]%s,[HDD]%s",
                        temporaryFolder1.getRoot().getAbsolutePath(),
                        temporaryFolder2.getRoot().getAbsolutePath()));
        partitionFactory.initialize(new Configuration(properties));

        expectedEvenDiskUsedCount(
                partitionFactory, temporaryFolder1, temporaryFolder2, StorageType.HDD);
    }

    static void expectedEvenDiskUsedCount(
            LocalFileMapPartitionFactory partitionFactory,
            TemporaryFolder temporaryFolder1,
            TemporaryFolder temporaryFolder2,
            StorageType expectedStorageType) {
        int numHdd1 = 0;
        int numHdd2 = 0;
        for (int i = 0; i < 100; ++i) {
            StorageMeta storageMeta = partitionFactory.getNextDataStorageMeta();
            if (StorageTestUtils.getStoragePath(temporaryFolder1)
                    .equals(storageMeta.getStoragePath())) {
                numHdd1++;
            } else if (StorageTestUtils.getStoragePath(temporaryFolder2)
                    .equals(storageMeta.getStoragePath())) {
                numHdd2++;
            }
            assertEquals(expectedStorageType, storageMeta.getStorageType());
        }
        assertTrue(numHdd1 == 50 && numHdd2 == 50);
    }

    /** Fake {@link StorageMeta} implementation for test. */
    static class FakeStorageMeta extends StorageMeta {

        private static final long serialVersionUID = -6718795518833646475L;

        private long numUsableSpaceBytes;

        private boolean isHealthy = true;

        public FakeStorageMeta(String storagePath, StorageType storageType) {
            super(storagePath, storageType);
        }

        @Override
        public long updateUsableStorageSpace() {
            return numUsableSpaceBytes;
        }

        @Override
        public boolean isHealthy() {
            return isHealthy;
        }

        public void updateUsableStorageSpace(long usableSpace) {
            numUsableSpaceBytes = usableSpace;
        }

        public void updateStorageHealthStatus(boolean isHealthy) {
            this.isHealthy = isHealthy;
        }
    }
}
