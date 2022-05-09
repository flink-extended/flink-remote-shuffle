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

package com.alibaba.flink.shuffle.coordinator.worker.metastore;

import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionMeta;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFile;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFileMeta;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Tests the behavior of {@link LocalShuffleMetaStore}. */
public class LocalShuffleMetaStoreTest {

    @ClassRule public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Test
    public void testAddDataPartition() throws Exception {
        File base = TEMP_FOLDER.newFolder();
        String basePath = base.getAbsolutePath() + "/";

        LocalShuffleMetaStore metaStore =
                new LocalShuffleMetaStore(Collections.singleton(basePath));

        DataPartitionMeta meta = randomMeta(basePath);
        metaStore.onPartitionCreated(meta);

        List<DataPartitionStatus> current = metaStore.listDataPartitions();
        assertEquals(1, current.size());
        assertEquals(
                new DataPartitionStatus(
                        meta.getJobID(),
                        new DataPartitionCoordinate(meta.getDataSetID(), meta.getDataPartitionID()),
                        false),
                current.get(0));

        File[] metaFiles = listMetaFiles(basePath);
        assertNotNull(metaFiles);
        assertEquals(1, metaFiles.length);
    }

    @Test
    public void testRestore() throws Exception {
        Set<String> basePaths = new HashSet<>();
        List<DataPartitionStatus> expectedDataPartitions = new ArrayList<>();

        for (int i = 0; i < 2; ++i) {
            File base = TEMP_FOLDER.newFolder();
            basePaths.add(base.getAbsolutePath() + "/");
        }
        LocalShuffleMetaStore metaStore = new LocalShuffleMetaStore(basePaths);

        // Adds some partition meta
        for (String basePath : basePaths) {
            for (int i = 0; i < 4; ++i) {
                DataPartitionMeta meta = randomMeta(basePath);
                metaStore.onPartitionCreated(meta);
                expectedDataPartitions.add(
                        new DataPartitionStatus(
                                meta.getJobID(),
                                new DataPartitionCoordinate(
                                        meta.getDataSetID(), meta.getDataPartitionID()),
                                false));
            }
        }

        // Now let's create a new meta store
        LocalShuffleMetaStore restoredMetastore = new LocalShuffleMetaStore(basePaths);
        List<DataPartitionStatus> dataPartitionStatus = restoredMetastore.listDataPartitions();
        assertThat(dataPartitionStatus, containsInAnyOrder(expectedDataPartitions.toArray()));
    }

    @Test
    public void testRestoreSkipNonExistMetaDir() throws Exception {
        File base = TEMP_FOLDER.newFolder();
        String basePath = base.getAbsolutePath() + "/";
        LocalShuffleMetaStore metaStore =
                new LocalShuffleMetaStore(Collections.singleton(basePath));
        assertEquals(0, metaStore.listDataPartitions().size());
    }

    @Test
    public void testRestoreRemoveSpoiledMetaFiles() throws Exception {
        File base = TEMP_FOLDER.newFolder();
        String basePath = base.getAbsolutePath() + "/";
        LocalShuffleMetaStore metaStore =
                new LocalShuffleMetaStore(Collections.singleton(basePath));
        metaStore.onPartitionCreated(randomMeta(basePath));

        File[] metaFiles = listMetaFiles(basePath);
        assertEquals(1, metaFiles.length);
        try (FileChannel channel = new FileOutputStream(metaFiles[0]).getChannel()) {
            channel.truncate(10);
        }

        LocalShuffleMetaStore restoredMetaStore =
                new LocalShuffleMetaStore(Collections.singleton(basePath));
        assertEquals(0, restoredMetaStore.listDataPartitions().size());
        assertEquals(0, listMetaFiles(basePath).length);
    }

    @Test
    public void testRemoveMetaFile() throws Exception {
        File base = TEMP_FOLDER.newFolder();
        String basePath = base.getAbsolutePath() + "/";
        LocalShuffleMetaStore metaStore =
                new LocalShuffleMetaStore(Collections.singleton(basePath));
        DataPartitionMeta meta = randomMeta(basePath);
        metaStore.onPartitionCreated(meta);

        // Removes the meta
        metaStore.onPartitionRemoved(meta);

        List<DataPartitionStatus> current = metaStore.listDataPartitions();
        assertEquals(1, current.size());
        assertEquals(
                new DataPartitionStatus(
                        meta.getJobID(),
                        new DataPartitionCoordinate(meta.getDataSetID(), meta.getDataPartitionID()),
                        true),
                current.get(0));
        // The meta file should has been removed
        File[] metaFiles = listMetaFiles(basePath);
        assertEquals(0, metaFiles.length);
    }

    public static LocalFileMapPartitionMeta randomMeta(String storagePath) {
        JobID jobId = RandomIDUtils.randomJobId();
        DataSetID dataSetId = RandomIDUtils.randomDataSetId();
        MapPartitionID dataPartitionId = RandomIDUtils.randomMapPartitionId();
        LocalMapPartitionFileMeta fileMeta =
                new LocalMapPartitionFileMeta(
                        storagePath + "test", 10, LocalMapPartitionFile.LATEST_STORAGE_VERSION);
        StorageMeta storageMeta = new StorageMeta(storagePath, StorageType.SSD, storagePath);
        return new LocalFileMapPartitionMeta(
                jobId, dataSetId, dataPartitionId, fileMeta, storageMeta);
    }

    private File[] listMetaFiles(String basePath) {
        return new File(basePath, LocalShuffleMetaStore.META_DIR_NAME).listFiles();
    }
}
