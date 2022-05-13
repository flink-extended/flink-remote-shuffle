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
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalFileMapPartitionFactory}. */
public class LocalFileMapPartitionFactoryTest {

    @Rule public final TemporaryFolder temporaryFolder1 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder2 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder3 = new TemporaryFolder();

    @Rule public final TemporaryFolder temporaryFolder4 = new TemporaryFolder();

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

        StorageMeta storageMeta1 =
                new LocalFileStorageMeta(
                        path1, StorageType.SSD, partitionFactory.getStorageNameFromPath(path1));
        StorageMeta storageMeta2 =
                new LocalFileStorageMeta(
                        path2, StorageType.HDD, partitionFactory.getStorageNameFromPath(path2));
        StorageMeta storageMeta3 =
                new LocalFileStorageMeta(
                        path3, StorageType.HDD, partitionFactory.getStorageNameFromPath(path3));

        Properties properties = new Properties();
        properties.setProperty(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(),
                String.format("[SSD]%s,[HDD]%s,%s", path1, path2, path3));
        properties.setProperty(StorageOptions.STORAGE_PREFERRED_TYPE.key(), "HDD");
        partitionFactory.initialize(new Configuration(properties));
        assertFalse(partitionFactory.isStorageSpaceLimited());

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
                            StorageTestUtils.NUM_REDUCE_PARTITIONS);
            assertEquals(path2, dataPartition.getPartitionMeta().getStorageMeta().getStoragePath());
        }
    }

    @Test
    public void testUpdateFreeStorageSpace() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        FakeStorageMeta[] storageMetas = addStorageMetas(partitionFactory);

        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());

        updateStorageFreeSpace(storageMetas, 4, partitionFactory);
        assertEquals(7, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(8, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageFreeSpace(storageMetas, 0, partitionFactory);
        assertEquals(3, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(4, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
    }

    @Test
    public void testUpdateFreeStorageSpaceWithUnhealthyStorage() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        FakeStorageMeta[] storageMetas = addStorageMetas(partitionFactory);
        updateStorageFreeSpace(storageMetas, 0, partitionFactory);

        updateStorageHealthStatus(storageMetas, 2, 4, false, partitionFactory);
        assertEquals(1, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(2, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageHealthStatus(storageMetas, 0, 2, false, partitionFactory);
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(0, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());

        updateStorageHealthStatus(storageMetas, 2, 4, true, partitionFactory);
        assertEquals(3, partitionFactory.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(4, partitionFactory.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
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

    static void updateStorageFreeSpace(
            FakeStorageMeta[] storageMetas,
            long base,
            LocalFileMapPartitionFactory partitionFactory) {
        for (int i = 1; i <= storageMetas.length; ++i) {
            storageMetas[i - 1].updateFreeStorageSpace(i + base);
        }
        partitionFactory.updateFreeStorageSpace();
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
        partitionFactory.updateFreeStorageSpace();
    }

    @Test
    public void testGetNextStorageMetaWithUnhealthyStorage() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        FakeStorageMeta[] storageMetas = addStorageMetas(partitionFactory);
        updateStorageFreeSpace(storageMetas, 0, partitionFactory);

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

    @Test
    public void testGetStorageName() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";

        Configuration configuration = new Configuration();
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS,
                String.format("[SSD]%s,[HDD]%s", path1, path2));
        partitionFactory.initialize(configuration);

        List<StorageMeta> ssdMetas = partitionFactory.getSsdStorageMetas();
        List<StorageMeta> hddMetas = partitionFactory.getHddStorageMetas();
        assertEquals(1, ssdMetas.size());
        assertEquals(1, hddMetas.size());
        assertEquals(ssdMetas.get(0).getStorageName(), hddMetas.get(0).getStorageName());
    }

    @Test
    public void testUpdateUsedStorageSpace() {
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";
        String path3 = temporaryFolder3.getRoot().getAbsolutePath() + "/";
        String path4 = temporaryFolder4.getRoot().getAbsolutePath() + "/";

        Map<String, String> storageNameMap = new HashMap<>();
        storageNameMap.put(path1, "storage1");
        storageNameMap.put(path2, "storage2");
        storageNameMap.put(path3, "storage3");
        storageNameMap.put(path4, "storage3");
        LocalFileMapPartitionFactory partitionFactory = new FakePartitionFactory(storageNameMap);

        Configuration configuration = new Configuration();
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS,
                String.format("[SSD]%s,[SSD]%s,[HDD]%s,[HDD]%s", path1, path2, path3, path4));
        partitionFactory.initialize(configuration);

        Map<String, Long> storageUsedBytes = new HashMap<>();
        partitionFactory.updateUsedStorageSpace(storageUsedBytes);
        assertUsedStorageSpace(partitionFactory, 0, 0, 0, 0, 0);

        storageUsedBytes.put("storage1", 1024L);
        partitionFactory.updateUsedStorageSpace(storageUsedBytes);
        assertUsedStorageSpace(partitionFactory, 0, 1024, 1024, 0, 0);

        storageUsedBytes.put("storage2", 2048L);
        partitionFactory.updateUsedStorageSpace(storageUsedBytes);
        assertUsedStorageSpace(partitionFactory, 0, 2048, 1024, 2048, 0);

        storageUsedBytes.put("storage3", 4096L);
        partitionFactory.updateUsedStorageSpace(storageUsedBytes);
        assertUsedStorageSpace(partitionFactory, 4096, 2048, 1024, 2048, 4096);
    }

    private void assertUsedStorageSpace(
            LocalFileMapPartitionFactory partitionFactory,
            long hddMaxUsedSpaceBytes,
            long ssdMaxUsedSpaceBytes,
            long storage1UsedSpaceBytes,
            long storage2UsedSpaceBytes,
            long storage3UsedSpaceBytes) {
        StorageSpaceInfo storageSpaceInfo = partitionFactory.getStorageSpaceInfo();
        assertEquals(hddMaxUsedSpaceBytes, storageSpaceInfo.getHddMaxUsedSpaceBytes());
        assertEquals(ssdMaxUsedSpaceBytes, storageSpaceInfo.getSsdMaxUsedSpaceBytes());

        List<StorageMeta> ssdMetas = partitionFactory.getSsdStorageMetas();
        assertEquals(storage1UsedSpaceBytes, ssdMetas.get(0).getUsedStorageSpace());
        assertEquals(storage2UsedSpaceBytes, ssdMetas.get(1).getUsedStorageSpace());

        List<StorageMeta> hddMetas = partitionFactory.getHddStorageMetas();
        assertEquals(storage3UsedSpaceBytes, hddMetas.get(0).getUsedStorageSpace());
        assertEquals(storage3UsedSpaceBytes, hddMetas.get(1).getUsedStorageSpace());
    }

    @Test
    public void testSkipOverusedStorage() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";
        String path3 = temporaryFolder3.getRoot().getAbsolutePath() + "/";

        Configuration configuration = new Configuration();
        configuration.setMemorySize(
                StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        configuration.setMemorySize(
                StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES, MemorySize.parse("1k"));
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS, String.format("[HDD]%s", path3));
        partitionFactory.initialize(configuration);

        StorageMeta storageMeta1 = new StorageMeta(path1, StorageType.SSD, "storage1");
        StorageMeta storageMeta2 = new StorageMeta(path2, StorageType.SSD, "storage2");
        storageMeta1.updateFreeStorageSpace();
        storageMeta2.updateFreeStorageSpace();
        partitionFactory.addSsdStorageMeta(storageMeta1);
        partitionFactory.addSsdStorageMeta(storageMeta2);

        storageMeta1.updateUsedStorageSpace(2048);
        for (int i = 0; i < 1024; ++i) {
            assertEquals(path2, partitionFactory.getNextDataStorageMeta().getStoragePath());
        }
    }

    @Test
    public void testSkipStorageWithoutEnoughSpace() {
        LocalFileMapPartitionFactory partitionFactory = new LocalFileMapPartitionFactory();
        String path1 = temporaryFolder1.getRoot().getAbsolutePath() + "/";
        String path2 = temporaryFolder2.getRoot().getAbsolutePath() + "/";
        String path3 = temporaryFolder3.getRoot().getAbsolutePath() + "/";

        Configuration configuration = new Configuration();
        configuration.setString(
                StorageOptions.STORAGE_LOCAL_DATA_DIRS, String.format("[HDD]%s", path3));
        partitionFactory.initialize(configuration);

        FakeStorageMeta storageMeta1 = new FakeStorageMeta(path1, StorageType.SSD);
        FakeStorageMeta storageMeta2 = new FakeStorageMeta(path2, StorageType.SSD);
        storageMeta1.updateFreeStorageSpace(1024);
        storageMeta2.updateFreeStorageSpace(Long.MAX_VALUE);
        partitionFactory.addSsdStorageMeta(storageMeta1);
        partitionFactory.addSsdStorageMeta(storageMeta2);

        for (int i = 0; i < 1024; ++i) {
            assertEquals(path2, partitionFactory.getNextDataStorageMeta().getStoragePath());
        }
    }

    /**
     * Fake {@link com.alibaba.flink.shuffle.core.storage.DataPartitionFactory} implementation for
     * test.
     */
    static class FakePartitionFactory extends LocalFileMapPartitionFactory {

        private final Map<String, String> storageNameMap;

        FakePartitionFactory(Map<String, String> storageNameMap) {
            this.storageNameMap = storageNameMap;
        }

        @Override
        public String getStorageNameFromPath(String storagePath) {
            String storageName = storageNameMap.get(storagePath);
            if (storageName == null) {
                throw new IllegalArgumentException("Unknown storage path.");
            }
            return storageName;
        }
    }

    /** Fake {@link StorageMeta} implementation for test. */
    static class FakeStorageMeta extends StorageMeta {

        private static final long serialVersionUID = -6718795518833646475L;

        private long numFreeSpaceBytes;

        private boolean isHealthy = true;

        public FakeStorageMeta(String storagePath, StorageType storageType) {
            super(storagePath, storageType, storagePath);
        }

        @Override
        public long getFreeStorageSpace() {
            return numFreeSpaceBytes;
        }

        @Override
        public long updateFreeStorageSpace() {
            return numFreeSpaceBytes;
        }

        @Override
        public boolean isHealthy() {
            return isHealthy;
        }

        public void updateFreeStorageSpace(long freeSpace) {
            numFreeSpaceBytes = freeSpace;
        }

        public void updateStorageHealthStatus(boolean isHealthy) {
            this.isHealthy = isHealthy;
        }
    }
}
