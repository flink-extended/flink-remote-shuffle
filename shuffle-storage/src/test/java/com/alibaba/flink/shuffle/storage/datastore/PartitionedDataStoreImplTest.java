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

package com.alibaba.flink.shuffle.storage.datastore;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.exception.DuplicatedPartitionException;
import com.alibaba.flink.shuffle.core.exception.PartitionNotFoundException;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;
import com.alibaba.flink.shuffle.storage.exception.ConcurrentWriteException;
import com.alibaba.flink.shuffle.storage.partition.HDDOnlyLocalFileMapPartitionFactory;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionMeta;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFile;
import com.alibaba.flink.shuffle.storage.partition.LocalMapPartitionFileMeta;
import com.alibaba.flink.shuffle.storage.partition.LocalReducePartitionFile;
import com.alibaba.flink.shuffle.storage.partition.SSDOnlyLocalFileMapPartitionFactory;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;
import com.alibaba.flink.shuffle.storage.utils.TestDataCommitListener;
import com.alibaba.flink.shuffle.storage.utils.TestDataListener;
import com.alibaba.flink.shuffle.storage.utils.TestDataRegionCreditListener;
import com.alibaba.flink.shuffle.storage.utils.TestFailureListener;
import com.alibaba.flink.shuffle.storage.utils.TestPartitionStateListener;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Tests for {@link PartitionedDataStoreImpl}. */
public class PartitionedDataStoreImplTest {

    @Rule public Timeout timeout = new Timeout(60, TimeUnit.SECONDS);

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private static final int numBuffersPerReducePartitions = 100;

    public PartitionedDataStoreImpl dataStore;

    public TestPartitionStateListener partitionStateListener;

    @Before
    public void before() {
        partitionStateListener = new TestPartitionStateListener();
        dataStore =
                StorageTestUtils.createPartitionedDataStore(
                        temporaryFolder.getRoot().getAbsolutePath(), partitionStateListener);
    }

    @After
    public void after() {
        dataStore.shutDown(true);
    }

    @Test
    public void testWriteEmptyDataPartition() throws Exception {
        StorageTestUtils.createEmptyDataPartition(dataStore);

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
        assertEquals(StorageTestUtils.getDefaultDataPartition(), dataStore.getStoredData());
    }

    @Test
    public void testReadEmptyDataPartition() throws Exception {
        StorageTestUtils.createEmptyDataPartition(dataStore);

        TestDataListener dataListener = new TestDataListener();
        DataPartitionReadingView readingView =
                dataStore.createDataPartitionReadingView(
                        new ReadingViewContext(
                                StorageTestUtils.DATA_SET_ID,
                                StorageTestUtils.MAP_PARTITION_ID,
                                0,
                                0,
                                dataListener,
                                StorageTestUtils.NO_OP_BACKLOG_LISTENER,
                                StorageTestUtils.NO_OP_FAILURE_LISTENER));

        dataListener.waitData(60000);

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertTrue(readingView.isFinished());
        assertNull(readingView.nextBuffer());
    }

    @Test(expected = PartitionNotFoundException.class)
    public void testReadNonExistentDataPartition() throws Exception {
        StorageTestUtils.createDataPartitionReadingView(dataStore, 0);
    }

    @Test(expected = PartitionNotFoundException.class)
    public void testDeleteIndexFileOfLocalFileReducePartition() throws Exception {
        StorageTestUtils.createEmptyDataPartition(dataStore);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalReducePartitionFile.INDEX_FILE_SUFFIX)) {
                Files.delete(file.toPath());
            }
        }

        StorageTestUtils.createDataPartitionReadingView(dataStore, 0);
    }

    @Test(expected = PartitionNotFoundException.class)
    public void testDeleteIndexFileOfLocalFileMapPartition() throws Exception {
        StorageTestUtils.createEmptyDataPartition(dataStore);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.INDEX_FILE_SUFFIX)) {
                Files.delete(file.toPath());
            }
        }

        StorageTestUtils.createDataPartitionReadingView(dataStore, 0);
    }

    @Test(expected = PartitionNotFoundException.class)
    public void testDeleteDataFileOfLocalFileMapPartition() throws Exception {
        StorageTestUtils.createEmptyDataPartition(dataStore);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.DATA_FILE_SUFFIX)) {
                Files.delete(file.toPath());
            }
        }

        StorageTestUtils.createDataPartitionReadingView(dataStore, 0);
    }

    @Test(expected = PartitionNotFoundException.class)
    public void testDeleteDataFileOfLocalFileReducePartition() throws Exception {
        StorageTestUtils.createEmptyDataPartition(dataStore);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalReducePartitionFile.DATA_FILE_SUFFIX)) {
                Files.delete(file.toPath());
            }
        }

        StorageTestUtils.createDataPartitionReadingView(dataStore, 0);
    }

    @Test(expected = PartitionNotFoundException.class)
    public void testReadUnfinishedDataPartition() throws Exception {
        DataProducerTask producerTask = new DataProducerTask(dataStore, 120, false);
        producerTask.start();

        StorageTestUtils.createDataPartitionReadingView(dataStore, producerTask.getPartitionID());
    }

    @Test
    public void testReleaseWhileWriting() throws Exception {
        CountDownLatch latch = produceData();

        dataStore.releaseDataPartition(
                StorageTestUtils.DATA_SET_ID,
                StorageTestUtils.MAP_PARTITION_ID,
                new ShuffleException("Test."));
        latch.await();

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertDataStoreEmpty(dataStore);
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
        assertEquals(1, partitionStateListener.getNumCreated());
        assertEquals(1, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testReleaseWhileReading() throws Exception {
        int numProducers = 1;
        int numRegions = 20;

        MapPartitionID[] mapDataPartitionIDS =
                produceData(dataStore, numProducers, numRegions, false);
        CountDownLatch latch = consumeData(mapDataPartitionIDS[0]);

        dataStore.releaseDataByJobID(StorageTestUtils.JOB_ID, new ShuffleException("Test."));
        latch.await();

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertDataStoreEmpty(dataStore);
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(numProducers, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testReleaseDataPartition() throws Exception {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitions = addRandomPartitions();
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitionIDs = new HashMap<>();
        for (JobID jobID : dataPartitions.keySet()) {
            dataPartitionIDs.put(jobID, new HashMap<>());
            for (DataSetID dataSetID : dataPartitions.get(jobID).keySet()) {
                dataPartitionIDs
                        .get(jobID)
                        .put(dataSetID, new HashSet<>(dataPartitions.get(jobID).get(dataSetID)));
            }
        }

        for (JobID jobID : dataPartitionIDs.keySet()) {
            for (DataSetID dataSetID : dataPartitionIDs.get(jobID).keySet()) {
                for (DataPartitionID partitionID : dataPartitionIDs.get(jobID).get(dataSetID)) {
                    dataPartitions.get(jobID).get(dataSetID).remove(partitionID);
                    if (dataPartitions.get(jobID).get(dataSetID).isEmpty()) {
                        dataPartitions.get(jobID).remove(dataSetID);
                    }
                    if (dataPartitions.get(jobID).isEmpty()) {
                        dataPartitions.remove(jobID);
                    }
                    dataStore.releaseDataPartition(
                            dataSetID, partitionID, new ShuffleException("Tests."));
                    assertStoredDataIsExpected(dataStore, dataPartitions);
                }
            }
        }
        assertEquals(0, partitionStateListener.getNumCreated());
        assertEquals(220, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testReleaseDataSet() throws Exception {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitions = addRandomPartitions();
        Map<JobID, Set<DataSetID>> dataSetIDs = new HashMap<>();
        for (JobID jobID : dataPartitions.keySet()) {
            dataSetIDs.put(jobID, new HashSet<>(dataPartitions.get(jobID).keySet()));
        }

        for (JobID jobID : dataSetIDs.keySet()) {
            for (DataSetID dataSetID : dataSetIDs.get(jobID)) {
                dataPartitions.get(jobID).remove(dataSetID);
                if (dataPartitions.get(jobID).isEmpty()) {
                    dataPartitions.remove(jobID);
                }
                dataStore.releaseDataSet(dataSetID, new ShuffleException("Test."));
                assertStoredDataIsExpected(dataStore, dataPartitions);
            }
        }
        assertEquals(0, partitionStateListener.getNumCreated());
        assertEquals(220, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testReleaseByJobID() throws Exception {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitions = addRandomPartitions();

        while (!dataPartitions.isEmpty()) {
            Set<JobID> jobIDS = new HashSet<>(dataPartitions.keySet());
            for (JobID jobID : jobIDS) {
                dataPartitions.remove(jobID);
                dataStore.releaseDataByJobID(jobID, new ShuffleException("Test."));
                assertStoredDataIsExpected(dataStore, dataPartitions);
            }
        }
        assertEquals(0, partitionStateListener.getNumCreated());
        assertEquals(220, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testOnErrorWhileWritingWithoutData() throws Exception {
        DataPartitionWritingView writingView =
                CommonUtils.checkNotNull(
                        StorageTestUtils.createDataPartitionWritingView(dataStore));
        writingView.onError(new ShuffleException("Test exception."));

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertDataStoreEmpty(dataStore);
        assertEquals(1, partitionStateListener.getNumCreated());
        assertEquals(1, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testCreateDataPartitions() throws Exception {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataSetsByJob =
                addRandomPartitions(
                        (jobID, dataSetID, mapPartitionID, partitionID) -> {
                            DataPartitionWritingView writingView =
                                    dataStore.createDataPartitionWritingView(
                                            new WritingViewContext(
                                                    jobID,
                                                    dataSetID,
                                                    partitionID,
                                                    mapPartitionID,
                                                    StorageTestUtils.NUM_MAP_PARTITIONS,
                                                    StorageTestUtils.NUM_REDUCE_PARTITIONS,
                                                    StorageTestUtils
                                                            .LOCAL_FILE_MAP_PARTITION_FACTORY,
                                                    StorageTestUtils.NO_OP_CREDIT_LISTENER,
                                                    StorageTestUtils.NO_OP_FAILURE_LISTENER));
                            TestDataCommitListener commitListener = new TestDataCommitListener();
                            writingView.finish(commitListener);
                            commitListener.waitForDataCommission();
                        });

        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> storedData = dataStore.getStoredData();
        assertEquals(dataSetsByJob, storedData);

        assertEquals(440, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
        assertEquals(220, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testCreateExistedDataPartition() throws Exception {
        TestFailureListener failureLister = new TestFailureListener();
        DataPartitionWritingView writingView =
                CommonUtils.checkNotNull(
                        StorageTestUtils.createDataPartitionWritingView(dataStore, failureLister));
        writingView.finish(StorageTestUtils.NO_OP_DATA_COMMIT_LISTENER);

        StorageTestUtils.createDataPartitionWritingView(dataStore, failureLister);
        assertTrue(failureLister.getFailure().getCause() instanceof ConcurrentWriteException);

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertEquals(1, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testAddDataPartitions() throws Throwable {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitions = addRandomPartitions();

        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> storedData = dataStore.getStoredData();
        assertEquals(dataPartitions, storedData);
        assertEquals(0, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testAddExistedDataPartition() throws Exception {
        LocalMapPartitionFileMeta fileMeta =
                StorageTestUtils.createLocalMapPartitionFileMeta(temporaryFolder, true);
        StorageMeta storageMeta = StorageTestUtils.getStorageMeta(temporaryFolder);
        DataPartitionMeta partitionMeta =
                StorageTestUtils.createLocalFileMapPartitionMeta(fileMeta, storageMeta);

        dataStore.addDataPartition(partitionMeta);
        assertThrows(
                DuplicatedPartitionException.class,
                () -> dataStore.addDataPartition(partitionMeta));
        assertEquals(0, partitionStateListener.getNumCreated());
        while (partitionStateListener.getNumRemoved() != 1) {
            // wait until succeed or timeout
            Thread.sleep(100);
        }
        assertFalse(dataStore.getStoredData().isEmpty());
    }

    @Test
    public void testAddFailedDataPartition() throws Exception {
        LocalMapPartitionFileMeta fileMeta =
                StorageTestUtils.createLocalMapPartitionFileMeta(temporaryFolder, false);
        StorageMeta storageMeta = StorageTestUtils.getStorageMeta(temporaryFolder);
        DataPartitionMeta partitionMeta =
                StorageTestUtils.createLocalFileMapPartitionMeta(fileMeta, storageMeta);

        assertThrows(ShuffleException.class, () -> dataStore.addDataPartition(partitionMeta));
        assertDataStoreEmpty(dataStore);
        assertEquals(0, partitionStateListener.getNumCreated());
        assertEquals(1, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testOnErrorWhileWriting() throws Exception {
        int numProducers = 10;
        int numRegions = 60;

        produceData(dataStore, numProducers, numRegions, true);
        assertDataStoreEmpty(dataStore);
        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(numProducers, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testOnErrorWhileReading() throws Exception {
        int numProducers = 1;
        int numRegions = 10;

        MapPartitionID[] mapDataPartitionIDS =
                produceData(dataStore, numProducers, numRegions, false);

        consumeData(dataStore, mapDataPartitionIDS, numRegions, 1, true);
        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testWriteReadLargeDataVolume() throws Exception {
        int numProducers = 1;
        int numRegions = 60;

        MapPartitionID[] mapDataPartitionIDS =
                produceData(dataStore, numProducers, numRegions, false);

        consumeData(dataStore, mapDataPartitionIDS, numRegions, 1, false);
        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testUpdateStorageInfo() throws Exception {
        produceData(dataStore, 1, 1, false);

        dataStore.updateFreeStorageSpace();
        dataStore.updateStorageHealthStatus();
        dataStore.updateUsedStorageSpace();

        Map<String, StorageSpaceInfo> storageSpaceInfos = dataStore.getStorageSpaceInfos();
        assertEquals(4, storageSpaceInfos.size());
        assertNull(storageSpaceInfos.get(SSDOnlyLocalFileMapPartitionFactory.class.getName()));
        assertEquals(
                storageSpaceInfos.get(LocalFileMapPartitionFactory.class.getName()),
                storageSpaceInfos.get(HDDOnlyLocalFileMapPartitionFactory.class.getName()));

        StorageSpaceInfo storageSpaceInfo =
                storageSpaceInfos.get(LocalFileMapPartitionFactory.class.getName());
        assertEquals(0, storageSpaceInfo.getSsdMaxFreeSpaceBytes());
        assertEquals(0, storageSpaceInfo.getSsdMaxUsedSpaceBytes());
        assertTrue(storageSpaceInfo.getHddMaxFreeSpaceBytes() > 0);
        assertEquals(32784200, storageSpaceInfo.getHddMaxUsedSpaceBytes());
    }

    @Test
    public void testWriteReadMultiplePartitions() throws Exception {
        int numProducers = 20;
        int numRegions = 3;

        MapPartitionID[] mapDataPartitionIDS =
                produceData(dataStore, numProducers, numRegions, false);

        consumeData(dataStore, mapDataPartitionIDS, numRegions, 1, false);
        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testReadMultipleChannels() throws Exception {
        int numProducers = 20;
        int numRegions = 3;

        MapPartitionID[] mapDataPartitionIDS =
                produceData(dataStore, numProducers, numRegions, false);

        consumeData(dataStore, mapDataPartitionIDS, numRegions, 3, false);
        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(0, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testReadDataPartitionError() throws Exception {
        int numProducers = 1;
        int numRegions = 60;

        MapPartitionID[] mapDataPartitionIDS =
                produceData(dataStore, numProducers, numRegions, false);
        CountDownLatch latch = consumeData(mapDataPartitionIDS[0]);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            Files.delete(file.toPath());
        }
        latch.await();

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
        assertEquals(1, dataStore.getStoredData().size());

        assertThrows(PartitionNotFoundException.class, () -> consumeData(mapDataPartitionIDS[0]));
        assertDataStoreEmpty(dataStore);
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);

        assertEquals(numProducers, partitionStateListener.getNumCreated());
        assertEquals(numProducers, partitionStateListener.getNumRemoved());
    }

    @Test
    public void testWriteDataPartitionError() throws Throwable {
        CountDownLatch latch = produceData();

        File[] files = CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles());
        while (files.length == 0) {
            files = CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles());
        }
        for (File file : files) {
            Files.delete(file.toPath());
        }
        latch.await();

        StorageTestUtils.assertNoBufferLeaking(dataStore);
        assertDataStoreEmpty(dataStore);
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles()).length);

        assertEquals(1, partitionStateListener.getNumCreated());
        assertEquals(1, partitionStateListener.getNumRemoved());
    }

    private CountDownLatch produceData() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ReducePartitionID reducePartitionID = new ReducePartitionID(0);

        TestDataRegionCreditListener creditListener = new TestDataRegionCreditListener();
        TestFailureListener failureLister = new TestFailureListener();

        DataPartitionWritingView writingView =
                CommonUtils.checkNotNull(
                        StorageTestUtils.createDataPartitionWritingView(
                                dataStore, creditListener, failureLister));

        Thread writingThread =
                new Thread(
                        () -> {
                            try {
                                int totalBuffers = 51200;
                                int numBuffers = 0;
                                int regionIndex = 0;
                                writingView.regionStarted(regionIndex, false);
                                while (!failureLister.isFailed() && numBuffers++ < totalBuffers) {
                                    Object credit = creditListener.take(100, regionIndex);
                                    if (credit != null) {
                                        Buffer buffer =
                                                writingView.getBufferSupplier().pollBuffer();
                                        buffer.writeBytes(StorageTestUtils.DATA_BYTES);
                                        writingView.onBuffer(
                                                buffer, regionIndex, reducePartitionID);
                                    }
                                }
                                writingView.regionFinished(regionIndex);
                                writingView.finish(StorageTestUtils.NO_OP_DATA_COMMIT_LISTENER);
                            } catch (Throwable ignored) {
                            }

                            latch.countDown();
                        });
        writingThread.start();
        return latch;
    }

    private CountDownLatch consumeData(MapPartitionID mapPartitionID) throws Exception {
        int numReducePartitions = StorageTestUtils.NUM_REDUCE_PARTITIONS;
        CountDownLatch latch = new CountDownLatch(numReducePartitions);

        DataPartitionReadingView[] readingViews = new DataPartitionReadingView[numReducePartitions];
        for (int reduceIndex = 0; reduceIndex < numReducePartitions; ++reduceIndex) {
            TestDataListener dataListener = new TestDataListener();
            TestFailureListener failureLister = new TestFailureListener();
            readingViews[reduceIndex] =
                    dataStore.createDataPartitionReadingView(
                            new ReadingViewContext(
                                    StorageTestUtils.DATA_SET_ID,
                                    mapPartitionID,
                                    reduceIndex,
                                    reduceIndex,
                                    dataListener,
                                    StorageTestUtils.NO_OP_BACKLOG_LISTENER,
                                    failureLister));

            int finalIndex = reduceIndex;
            Thread readingThread =
                    new Thread(
                            () -> {
                                DataPartitionReadingView readingView = readingViews[finalIndex];
                                try {
                                    while (!readingView.isFinished() && !failureLister.isFailed()) {
                                        BufferWithBacklog buffer =
                                                readingViews[finalIndex].nextBuffer();
                                        if (buffer != null) {
                                            BufferUtils.recycleBuffer(buffer.getBuffer());
                                        }
                                    }
                                } catch (Throwable ignored) {
                                }
                                latch.countDown();
                            });
            readingThread.start();
        }
        return latch;
    }

    private MapPartitionID[] produceData(
            PartitionedDataStoreImpl dataStore, int numProducers, int numRegions, boolean isError)
            throws Exception {
        DataProducerTask[] producerTasks = new DataProducerTask[numProducers];

        for (int i = 0; i < numProducers; ++i) {
            DataProducerTask producerTask = new DataProducerTask(dataStore, numRegions, isError);
            producerTasks[i] = producerTask;
            producerTask.start();
        }

        for (DataProducerTask producerTask : producerTasks) {
            producerTask.join();
            assertEquals(isError, producerTask.isFailed());
            assertFalse(producerTask.getFailureListener().isFailed());
        }

        StorageTestUtils.assertNoBufferLeaking(dataStore);

        MapPartitionID[] mapDataPartitionIDS = new MapPartitionID[numProducers];
        for (int i = 0; i < numProducers; ++i) {
            mapDataPartitionIDS[i] = producerTasks[i].getPartitionID();
        }

        if (isError) {
            assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
            assertEquals(0, dataStore.getStoredData().size());
        } else {
            Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataPartitions =
                    StorageTestUtils.getDataPartitions(Arrays.asList(mapDataPartitionIDS));
            Map<JobID, Map<DataSetID, Set<DataPartitionID>>> storedData = dataStore.getStoredData();
            assertEquals(dataPartitions, storedData);
        }

        return mapDataPartitionIDS;
    }

    private void consumeData(
            PartitionedDataStoreImpl dataStore,
            MapPartitionID[] mapDataPartitionIDS,
            int numRegions,
            int numPartitions,
            boolean isError)
            throws Exception {
        List<DataConsumerTask> consumerTasks = new ArrayList<>();
        for (int partitionIndex = 0; partitionIndex < StorageTestUtils.NUM_REDUCE_PARTITIONS; ) {
            DataConsumerTask consumerTask =
                    new DataConsumerTask(
                            dataStore,
                            partitionIndex,
                            Math.min(
                                    partitionIndex + numPartitions - 1,
                                    StorageTestUtils.NUM_REDUCE_PARTITIONS - 1),
                            mapDataPartitionIDS,
                            numRegions,
                            isError);
            partitionIndex += numPartitions;
            consumerTasks.add(consumerTask);
            consumerTask.start();
        }

        for (DataConsumerTask consumerTask : consumerTasks) {
            consumerTask.join();
            assertEquals(isError, consumerTask.isFailed());
            assertFalse(consumerTask.getFailureListener().isFailed());
        }

        StorageTestUtils.assertNoBufferLeaking(dataStore);
    }

    private Map<JobID, Map<DataSetID, Set<DataPartitionID>>> addRandomPartitions()
            throws Exception {
        LocalMapPartitionFileMeta fileMeta =
                StorageTestUtils.createLocalMapPartitionFileMeta(temporaryFolder, true);
        StorageMeta storageMeta = StorageTestUtils.getStorageMeta(temporaryFolder);

        return addRandomPartitions(
                (jobID, dataSetID, mapPartitionID, partitionID) -> {
                    DataPartitionMeta partitionMeta =
                            new LocalFileMapPartitionMeta(
                                    jobID, dataSetID, mapPartitionID, fileMeta, storageMeta);
                    dataStore.addDataPartition(partitionMeta);
                });
    }

    private Map<JobID, Map<DataSetID, Set<DataPartitionID>>> addRandomPartitions(
            AddPartitionFunction function) throws Exception {
        int numJobs = 10;
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataGenerated = new HashMap<>();

        for (int jobIndex = 1; jobIndex <= numJobs; ++jobIndex) {
            JobID jobID = new JobID(CommonUtils.randomBytes(16));
            Map<DataSetID, Set<DataPartitionID>> dataSetPartitions = new HashMap<>();
            dataGenerated.put(jobID, dataSetPartitions);

            for (int dataSetIndex = 1; dataSetIndex <= jobIndex; ++dataSetIndex) {
                DataSetID dataSetID = new DataSetID(CommonUtils.randomBytes(16));
                Set<DataPartitionID> dataPartitions = new HashSet<>();
                dataSetPartitions.put(dataSetID, dataPartitions);

                for (int partitionIndex = 1; partitionIndex <= dataSetIndex; ++partitionIndex) {
                    MapPartitionID partitionID = new MapPartitionID(CommonUtils.randomBytes(16));
                    dataPartitions.add(partitionID);

                    function.addDataPartition(jobID, dataSetID, partitionID, partitionID);
                }
            }
        }

        return dataGenerated;
    }

    private void assertDataStoreEmpty(PartitionedDataStoreImpl dataStore) throws Exception {
        while (!dataStore.getStoredData().isEmpty()) {
            Thread.sleep(100);
        }
    }

    private void assertStoredDataIsExpected(
            PartitionedDataStoreImpl dataStore,
            Map<JobID, Map<DataSetID, Set<DataPartitionID>>> expected)
            throws Exception {
        while (!dataStore.getStoredData().equals(expected)) {
            Thread.sleep(10);
        }
    }

    private static class DataProducerTask extends Thread {

        private final TestDataCommitListener commitListener = new TestDataCommitListener();

        private final MapPartitionID partitionID = new MapPartitionID(CommonUtils.randomBytes(16));

        private final TestDataRegionCreditListener creditListener =
                new TestDataRegionCreditListener();

        private final TestFailureListener failureListener = new TestFailureListener();

        private final PartitionedDataStoreImpl dataStore;

        private final boolean triggerWritingViewError;

        private final int numRegions;

        private volatile Throwable failure;

        private DataProducerTask(
                PartitionedDataStoreImpl dataStore,
                int numRegions,
                boolean triggerWritingViewError) {
            CommonUtils.checkArgument(dataStore != null, "Must be not null.");
            CommonUtils.checkArgument(numRegions > 0, "Must be positive.");

            this.dataStore = dataStore;
            this.numRegions = numRegions;
            this.triggerWritingViewError = triggerWritingViewError;
            setName("Data Producer Task");
        }

        @Override
        public void run() {
            try {
                DataPartitionWritingView writingView =
                        dataStore.createDataPartitionWritingView(
                                new WritingViewContext(
                                        StorageTestUtils.JOB_ID,
                                        StorageTestUtils.DATA_SET_ID,
                                        partitionID,
                                        partitionID,
                                        StorageTestUtils.NUM_MAP_PARTITIONS,
                                        StorageTestUtils.NUM_REDUCE_PARTITIONS,
                                        StorageTestUtils.LOCAL_FILE_MAP_PARTITION_FACTORY,
                                        creditListener,
                                        failureListener));

                for (int regionID = 0; regionID < numRegions; ++regionID) {
                    int numReducePartitions = StorageTestUtils.NUM_REDUCE_PARTITIONS;
                    writingView.regionStarted(regionID, false);

                    for (int reduceID = 0; reduceID < numReducePartitions; ++reduceID) {
                        for (int i = 0; i < numBuffersPerReducePartitions; ++i) {
                            creditListener.take(0, regionID);
                            Buffer buffer = writingView.getBufferSupplier().pollBuffer();
                            buffer.writeBytes(StorageTestUtils.DATA_BYTES);
                            writingView.onBuffer(buffer, regionID, new ReducePartitionID(reduceID));
                        }

                        if (triggerWritingViewError) {
                            writingView.onError(new ShuffleException("Test exception."));
                            break;
                        }
                    }
                    writingView.regionFinished(regionID);
                }
                writingView.finish(commitListener);
                commitListener.waitForDataCommission();
            } catch (Throwable throwable) {
                failure = throwable;
            }
        }

        public MapPartitionID getPartitionID() {
            return partitionID;
        }

        public boolean isFailed() {
            return failure != null || failureListener.isFailed();
        }

        public TestFailureListener getFailureListener() {
            return failureListener;
        }
    }

    private static class DataConsumerTask extends Thread {

        private final TestDataListener dataListener = new TestDataListener();

        private final TestFailureListener failureListener = new TestFailureListener();

        private final PartitionedDataStoreImpl dataStore;

        private final MapPartitionID[] mapDataPartitionIDS;

        private final int startPartitionIndex;

        private final int endPartitionIndex;

        private final int numRegions;

        private final boolean isError;

        private volatile Throwable failure;

        private DataConsumerTask(
                PartitionedDataStoreImpl dataStore,
                int startPartitionIndex,
                int endPartitionIndex,
                MapPartitionID[] mapDataPartitionIDS,
                int numRegions,
                boolean isError) {
            CommonUtils.checkArgument(dataStore != null, "Must be not null.");
            CommonUtils.checkArgument(mapDataPartitionIDS != null, "Must be not null.");
            CommonUtils.checkArgument(numRegions > 0, "Must be positive.");

            this.dataStore = dataStore;
            this.mapDataPartitionIDS = mapDataPartitionIDS;
            this.startPartitionIndex = startPartitionIndex;
            this.endPartitionIndex = endPartitionIndex;
            this.numRegions = numRegions;
            this.isError = isError;
            setName("Data Consumer Task");
        }

        @Override
        public void run() {
            try {
                Queue<DataPartitionReadingView> readingViews = new ArrayDeque<>();
                Map<DataPartitionReadingView, Integer> bufferCounters = new HashMap<>();

                for (MapPartitionID mapDataPartitionID : mapDataPartitionIDS) {
                    DataPartitionReadingView readingView =
                            dataStore.createDataPartitionReadingView(
                                    new ReadingViewContext(
                                            StorageTestUtils.DATA_SET_ID,
                                            mapDataPartitionID,
                                            startPartitionIndex,
                                            endPartitionIndex,
                                            dataListener,
                                            StorageTestUtils.NO_OP_BACKLOG_LISTENER,
                                            failureListener));
                    readingViews.add(readingView);
                    bufferCounters.put(readingView, 0);
                }

                while (!readingViews.isEmpty()) {
                    DataPartitionReadingView readingView = readingViews.poll();
                    BufferWithBacklog bufferWithBacklog;

                    boolean isErrorTriggered = false;
                    while ((bufferWithBacklog = readingView.nextBuffer()) != null) {
                        assertEquals(
                                ByteBuffer.wrap(StorageTestUtils.DATA_BYTES),
                                bufferWithBacklog.getBuffer().nioBuffer());
                        bufferCounters.put(readingView, bufferCounters.get(readingView) + 1);
                        BufferUtils.recycleBuffer(bufferWithBacklog.getBuffer());

                        if (isError) {
                            readingView.onError(new ShuffleException("Test exception."));
                            isErrorTriggered = true;
                            break;
                        }
                    }

                    if (!isErrorTriggered && !readingView.isFinished()) {
                        dataListener.waitData(100);
                        readingViews.add(readingView);
                    }
                }

                if (startPartitionIndex == endPartitionIndex) {
                    int numBuffersPerProducer = numBuffersPerReducePartitions * numRegions;
                    for (Integer numBuffers : bufferCounters.values()) {
                        assertEquals((Integer) numBuffersPerProducer, numBuffers);
                    }
                }
            } catch (Throwable throwable) {
                failure = throwable;
            }
        }

        public boolean isFailed() {
            return failure != null || failureListener.isFailed();
        }

        public TestFailureListener getFailureListener() {
            return failureListener;
        }
    }

    private interface AddPartitionFunction {

        void addDataPartition(
                JobID jobID,
                DataSetID dataSetID,
                MapPartitionID mapPartitionID,
                DataPartitionID partitionID)
                throws Exception;
    }
}
