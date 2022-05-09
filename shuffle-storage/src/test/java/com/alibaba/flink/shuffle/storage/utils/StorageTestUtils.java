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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataCommitListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.listener.PartitionStateListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.storage.datastore.NoOpPartitionedDataStore;
import com.alibaba.flink.shuffle.storage.datastore.PartitionedDataStoreImpl;
import com.alibaba.flink.shuffle.storage.partition.BufferOrMarker;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionMeta;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFile;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFileMeta;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFileWriter;

import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

/** Utility methods commonly used by tests of storage layer. */
public class StorageTestUtils {

    public static final int NUM_REDUCE_PARTITIONS = 10;

    public static final int DATA_BUFFER_SIZE = 32 * 1024;

    public static final byte[] DATA_BYTES = CommonUtils.randomBytes(DATA_BUFFER_SIZE);

    public static final PartitionedDataStore NO_OP_PARTITIONED_DATA_STORE =
            new NoOpPartitionedDataStore();

    public static final JobID JOB_ID = new JobID(CommonUtils.randomBytes(16));

    public static final DataSetID DATA_SET_ID = new DataSetID(CommonUtils.randomBytes(16));

    public static final FailureListener NO_OP_FAILURE_LISTENER = (ignored) -> {};

    public static final MapPartitionID MAP_PARTITION_ID =
            new MapPartitionID(CommonUtils.randomBytes(16));

    public static final DataRegionCreditListener NO_OP_CREDIT_LISTENER = (ignored1, ignored2) -> {};

    public static final DataListener NO_OP_DATA_LISTENER = () -> {};

    public static final BacklogListener NO_OP_BACKLOG_LISTENER = (ignored) -> {};

    public static final BufferRecycler NO_OP_BUFFER_RECYCLER = (ignored) -> {};

    public static final DataCommitListener NO_OP_DATA_COMMIT_LISTENER = () -> {};

    public static final String LOCAL_FILE_MAP_PARTITION_FACTORY =
            LocalFileMapPartitionFactory.class.getName();

    public static LocalMapPartitionFileMeta createLocalMapPartitionFileMeta() {
        String basePath = CommonUtils.randomHexString(128) + "/";
        String fileName = CommonUtils.randomHexString(32);
        Random random = new Random();
        int numReducePartitions = random.nextInt(Integer.MAX_VALUE) + 1;

        return new LocalMapPartitionFileMeta(
                basePath + fileName,
                numReducePartitions,
                LocalMapPartitionFile.LATEST_STORAGE_VERSION);
    }

    public static LocalMapPartitionFile createLocalMapPartitionFile(String baseDir) {
        String basePath = baseDir + "/";
        String fileName = CommonUtils.randomHexString(32);
        LocalMapPartitionFileMeta fileMeta =
                new LocalMapPartitionFileMeta(
                        basePath + fileName,
                        NUM_REDUCE_PARTITIONS,
                        LocalMapPartitionFile.LATEST_STORAGE_VERSION);
        return new LocalMapPartitionFile(fileMeta, 3, false);
    }

    public static void writeLocalMapPartitionFile(
            LocalMapPartitionFile partitionFile,
            int numRegions,
            int numReducePartitions,
            int numBuffers,
            boolean withEmptyReducePartitions,
            boolean dataChecksumEnabled)
            throws Exception {
        writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                numReducePartitions,
                numBuffers,
                withEmptyReducePartitions,
                -1,
                dataChecksumEnabled);
    }

    public static void writeLocalMapPartitionFile(
            LocalMapPartitionFile partitionFile,
            int numRegions,
            int numReducePartitions,
            int numBuffers,
            boolean withEmptyReducePartitions,
            int broadcastRegionIndex,
            boolean dataChecksumEnabled)
            throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(partitionFile, 2, dataChecksumEnabled);
        fileWriter.open();
        for (int regionIndex = 0; regionIndex < numRegions; ++regionIndex) {
            if (regionIndex == broadcastRegionIndex) {
                fileWriter.startRegion(true);
                for (int bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex) {
                    fileWriter.writeBuffer(createDataBuffer(createRandomData(), 0));
                }
            } else {
                fileWriter.startRegion(false);
                for (int partition = 0; partition < numReducePartitions; ++partition) {
                    if (withEmptyReducePartitions && partition % 2 == 0) {
                        continue;
                    }
                    for (int bufferIndex = 0; bufferIndex < numBuffers; ++bufferIndex) {
                        fileWriter.writeBuffer(createDataBuffer(createRandomData(), partition));
                    }
                }
            }
            fileWriter.finishRegion();
        }
        fileWriter.finishWriting();
    }

    public static ByteBuffer createRandomData() {
        ByteBuffer data = ByteBuffer.allocateDirect(DATA_BUFFER_SIZE);
        data.put(DATA_BYTES);
        data.flip();
        return data;
    }

    public static BufferOrMarker.DataBuffer createDataBuffer(ByteBuffer data, int channelIndex) {
        return new BufferOrMarker.DataBuffer(
                StorageTestUtils.MAP_PARTITION_ID,
                new ReducePartitionID(channelIndex),
                new Buffer(data, StorageTestUtils.NO_OP_BUFFER_RECYCLER, data.remaining()));
    }

    public static void assertNoBufferLeaking() throws Exception {
        assertNoBufferLeaking(NO_OP_PARTITIONED_DATA_STORE);
    }

    public static void assertNoBufferLeaking(PartitionedDataStore dataStore) throws Exception {
        assertNoBufferLeaking(dataStore.getWritingBufferDispatcher());
        assertNoBufferLeaking(dataStore.getReadingBufferDispatcher());
    }

    public static void assertNoBufferLeaking(BufferDispatcher bufferDispatcher) throws Exception {
        while (bufferDispatcher.numAvailableBuffers() != bufferDispatcher.numTotalBuffers()) {
            Thread.sleep(100);
        }
    }

    public static PartitionedDataStoreImpl createPartitionedDataStore(
            String storageDir, PartitionStateListener partitionStateListener) {
        Properties properties = new Properties();
        properties.setProperty(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_READING.key(),
                MemoryOptions.MIN_VALID_MEMORY_SIZE.getBytes() + "b");
        properties.setProperty(
                MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING.key(),
                MemoryOptions.MIN_VALID_MEMORY_SIZE.getBytes() + "b");
        properties.setProperty(StorageOptions.STORAGE_LOCAL_DATA_DIRS.key(), storageDir);
        Configuration configuration = new Configuration(properties);
        return new PartitionedDataStoreImpl(configuration, partitionStateListener);
    }

    public static void createEmptyDataPartition(PartitionedDataStore dataStore) throws Exception {
        DataPartitionWritingView writingView =
                CommonUtils.checkNotNull(createDataPartitionWritingView(dataStore));

        TestDataCommitListener commitListener = new TestDataCommitListener();
        writingView.finish(commitListener);
        commitListener.waitForDataCommission();
    }

    public static DataPartitionWritingView createDataPartitionWritingView(
            PartitionedDataStore dataStore) throws Exception {
        return dataStore.createDataPartitionWritingView(
                new WritingViewContext(
                        JOB_ID,
                        DATA_SET_ID,
                        MAP_PARTITION_ID,
                        MAP_PARTITION_ID,
                        NUM_REDUCE_PARTITIONS,
                        LOCAL_FILE_MAP_PARTITION_FACTORY,
                        NO_OP_CREDIT_LISTENER,
                        NO_OP_FAILURE_LISTENER));
    }

    public static DataPartitionWritingView createDataPartitionWritingView(
            PartitionedDataStore dataStore,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener)
            throws Exception {
        return dataStore.createDataPartitionWritingView(
                new WritingViewContext(
                        JOB_ID,
                        DATA_SET_ID,
                        MAP_PARTITION_ID,
                        MAP_PARTITION_ID,
                        NUM_REDUCE_PARTITIONS,
                        LOCAL_FILE_MAP_PARTITION_FACTORY,
                        dataRegionCreditListener,
                        failureListener));
    }

    public static DataPartitionWritingView createDataPartitionWritingView(
            PartitionedDataStore dataStore, FailureListener failureListener) throws Exception {
        return dataStore.createDataPartitionWritingView(
                new WritingViewContext(
                        JOB_ID,
                        DATA_SET_ID,
                        MAP_PARTITION_ID,
                        MAP_PARTITION_ID,
                        NUM_REDUCE_PARTITIONS,
                        LOCAL_FILE_MAP_PARTITION_FACTORY,
                        NO_OP_CREDIT_LISTENER,
                        failureListener));
    }

    public static Map<JobID, Map<DataSetID, Set<DataPartitionID>>> getDefaultDataPartition() {
        return getDataPartitions(Collections.singletonList(MAP_PARTITION_ID));
    }

    public static Map<JobID, Map<DataSetID, Set<DataPartitionID>>> getDataPartitions(
            List<MapPartitionID> mapPartitionIDS) {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitions = new HashMap<>();
        Map<DataSetID, Set<DataPartitionID>> dataSetPartitions = new HashMap<>();
        dataPartitions.put(JOB_ID, dataSetPartitions);
        dataSetPartitions.put(DATA_SET_ID, new HashSet<>(mapPartitionIDS));
        return dataPartitions;
    }

    public static void createDataPartitionReadingView(
            PartitionedDataStore dataStore, MapPartitionID mapPartitionID) throws Exception {
        dataStore.createDataPartitionReadingView(
                new ReadingViewContext(
                        DATA_SET_ID,
                        mapPartitionID,
                        0,
                        0,
                        NO_OP_DATA_LISTENER,
                        NO_OP_BACKLOG_LISTENER,
                        NO_OP_FAILURE_LISTENER));
    }

    public static void createDataPartitionReadingView(
            PartitionedDataStore dataStore, int reduceIndex) throws Exception {
        dataStore.createDataPartitionReadingView(
                new ReadingViewContext(
                        DATA_SET_ID,
                        MAP_PARTITION_ID,
                        reduceIndex,
                        reduceIndex,
                        NO_OP_DATA_LISTENER,
                        NO_OP_BACKLOG_LISTENER,
                        NO_OP_FAILURE_LISTENER));
    }

    public static LocalMapPartitionFileMeta createLocalMapPartitionFileMeta(
            TemporaryFolder temporaryFolder, boolean createFile) throws IOException {
        String fileName = CommonUtils.randomHexString(32);
        if (createFile) {
            temporaryFolder.newFile(fileName + LocalMapPartitionFile.DATA_FILE_SUFFIX);
            temporaryFolder.newFile(fileName + LocalMapPartitionFile.INDEX_FILE_SUFFIX);
        }
        return new LocalMapPartitionFileMeta(
                getStoragePath(temporaryFolder) + fileName,
                NUM_REDUCE_PARTITIONS,
                LocalMapPartitionFile.LATEST_STORAGE_VERSION);
    }

    public static LocalFileMapPartitionMeta createLocalFileMapPartitionMeta(
            LocalMapPartitionFileMeta fileMeta, StorageMeta storageMeta) {
        return new LocalFileMapPartitionMeta(
                JOB_ID, DATA_SET_ID, MAP_PARTITION_ID, fileMeta, storageMeta);
    }

    public static StorageMeta getStorageMeta() {
        return new StorageMeta("/tmp/", StorageType.SSD, "/tmp/");
    }

    public static StorageMeta getStorageMeta(TemporaryFolder temporaryFolder) {
        String storagePath = getStoragePath(temporaryFolder);
        return new StorageMeta(storagePath, StorageType.SSD, storagePath);
    }

    public static String getStoragePath(TemporaryFolder temporaryFolder) {
        return temporaryFolder.getRoot().getAbsolutePath() + "/";
    }
}
