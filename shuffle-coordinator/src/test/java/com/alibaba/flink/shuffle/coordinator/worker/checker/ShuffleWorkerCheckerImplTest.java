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

package com.alibaba.flink.shuffle.coordinator.worker.checker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.utils.EmptyPartitionedDataStore;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.listener.PartitionStateListener;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataStoreStatistics;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.datastore.PartitionedDataStoreImpl;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ShuffleWorkerCheckerImpl}. */
public class ShuffleWorkerCheckerImplTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private PartitionedDataStoreImpl dataStore;

    @Before
    public void before() {
        dataStore =
                createPartitionedDataStore(
                        temporaryFolder.getRoot().getAbsolutePath(),
                        new NoOpPartitionStateListener());
    }

    @After
    public void clear() {
        dataStore.shutDown(true);
        temporaryFolder.delete();
    }

    @Test
    public void testInitWorkerCheckerWithEmptyStore() {
        ShuffleWorkerCheckerImpl workerChecker =
                new ShuffleWorkerCheckerImpl(new Configuration(), new EmptyPartitionedDataStore());
        assertEquals(0, workerChecker.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
        assertEquals(0, workerChecker.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(
                DataStoreStatistics.EMPTY_DATA_STORE_STATISTICS,
                workerChecker.getDataStoreStatistics());
    }

    @Test
    public void testInitWorkerCheckerWithHddStore() {
        ShuffleWorkerCheckerImpl workerChecker =
                new ShuffleWorkerCheckerImpl(new Configuration(), dataStore);
        assertTrue(workerChecker.getStorageSpaceInfo().getHddMaxFreeSpaceBytes() > 0);
        assertEquals(0, workerChecker.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
        assertEquals(
                DataStoreStatistics.EMPTY_DATA_STORE_STATISTICS,
                workerChecker.getDataStoreStatistics());
    }

    @Test
    public void testInitWorkerCheckerWithSsdStore() {
        PartitionedDataStoreImpl dataStore =
                createPartitionedDataStore(
                        temporaryFolder.getRoot().getAbsolutePath(),
                        new NoOpPartitionStateListener(),
                        StorageType.SSD);
        ShuffleWorkerCheckerImpl workerChecker =
                new ShuffleWorkerCheckerImpl(new Configuration(), dataStore);
        assertEquals(0, workerChecker.getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
        assertTrue(workerChecker.getStorageSpaceInfo().getSsdMaxFreeSpaceBytes() > 0);
        assertEquals(
                DataStoreStatistics.EMPTY_DATA_STORE_STATISTICS,
                workerChecker.getDataStoreStatistics());
    }

    @Test
    public void testWrongUpdateIntervalArgument() {
        Configuration configuration = new Configuration();
        configuration.setString("remote-shuffle.storage.check-update-period", "0");
        try {
            new ShuffleWorkerCheckerImpl(configuration, dataStore);
        } catch (Throwable t) {
            assertTrue(
                    ExceptionUtils.findThrowable(
                                    t,
                                    throwable ->
                                            throwable
                                                    .getMessage()
                                                    .contains("The storage check interval"))
                            .isPresent());
        }
    }

    private static class NoOpPartitionStateListener implements PartitionStateListener {
        @Override
        public void onPartitionCreated(DataPartitionMeta partitionMeta) {}

        @Override
        public void onPartitionRemoved(DataPartitionMeta partitionMeta) {}
    }

    private static PartitionedDataStoreImpl createPartitionedDataStore(
            String storageDir, PartitionStateListener partitionStateListener) {
        return createPartitionedDataStore(storageDir, partitionStateListener, StorageType.HDD);
    }

    private static PartitionedDataStoreImpl createPartitionedDataStore(
            String storageDir,
            PartitionStateListener partitionStateListener,
            StorageType storageType) {
        Properties properties = new Properties();
        properties.setProperty(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_READING.key(),
                MemoryOptions.MIN_VALID_MEMORY_SIZE.getBytes() + "b");
        properties.setProperty(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING.key(),
                MemoryOptions.MIN_VALID_MEMORY_SIZE.getBytes() + "b");
        if (storageType.equals(StorageType.HDD)) {
            properties.setProperty(StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(), storageDir);
        } else {
            properties.setProperty(
                    StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(), "[SSD]" + storageDir);
        }
        Configuration configuration = new Configuration(properties);
        return new PartitionedDataStoreImpl(configuration, partitionStateListener);
    }
}
