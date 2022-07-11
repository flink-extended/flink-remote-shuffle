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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;
import com.alibaba.flink.shuffle.storage.utils.TestDataCommitListener;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalReducePartitionFileWriter}. */
@RunWith(Parameterized.class)
public class LocalReducePartitionFileWriterTest {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalReducePartitionFileWriterTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testWriteData() throws Exception {
        LocalReducePartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalReducePartitionFile(
                partitionFile,
                10,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                10,
                false,
                dataChecksumEnabled);

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testWriteEmptyData() throws Exception {
        LocalReducePartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalReducePartitionFile(
                partitionFile,
                0,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                0,
                false,
                dataChecksumEnabled);

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);

        LocalReducePartitionFileMeta fileMeta = partitionFile.getFileMeta();
        assertEquals(0, new File(fileMeta.getDataFilePath().toString()).length());
        assertEquals(
                LocalReducePartitionFile.INDEX_DATA_CHECKSUM_SIZE + 4 + 16,
                new File(fileMeta.getIndexFilePath().toString()).length());
    }

    @Test
    public void testWriteWithEmptyReducePartition() throws Exception {
        LocalReducePartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalReducePartitionFile(
                partitionFile,
                10,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                10,
                true,
                dataChecksumEnabled);

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testWriteEmptyBuffer() throws Exception {
        LocalReducePartitionFile partitionFile = createPartitionFile();
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(partitionFile, 2, dataChecksumEnabled);
        fileWriter.open();

        fileWriter.startRegion(false, StorageTestUtils.MAP_PARTITION_ID);
        ByteBuffer data = ByteBuffer.allocateDirect(0);
        fileWriter.writeBuffer(StorageTestUtils.createDataBuffer(data, 0), false);
        fileWriter.finishRegion(StorageTestUtils.MAP_PARTITION_ID);

        fileWriter.prepareFinishWriting(
                new BufferOrMarker.InputFinishedMarker(
                        StorageTestUtils.MAP_PARTITION_ID, new TestDataCommitListener()));
        fileWriter.closeWriting();

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);

        LocalReducePartitionFileMeta fileMeta = partitionFile.getFileMeta();
        assertEquals(0, new File(fileMeta.getDataFilePath().toString()).length());
        assertEquals(
                LocalReducePartitionFile.INDEX_DATA_CHECKSUM_SIZE + 4 + 16,
                new File(fileMeta.getIndexFilePath().toString()).length());
    }

    @Test(expected = IllegalStateException.class)
    public void testStartRegionAfterClose() throws Exception {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.startRegion(false, StorageTestUtils.MAP_PARTITION_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishRegionAfterClose() throws Exception {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.finishRegion(StorageTestUtils.MAP_PARTITION_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteBufferAfterClose() throws Exception {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 1, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0), false);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishWritingAfterClose() throws Exception {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.prepareFinishWriting(
                new BufferOrMarker.InputFinishedMarker(
                        StorageTestUtils.MAP_PARTITION_ID, new TestDataCommitListener()));
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteNotInPartitionIndexOrder() throws IOException {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.startRegion(false, StorageTestUtils.MAP_PARTITION_ID);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 5), false);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0), false);
    }

    @Test(expected = IllegalStateException.class)
    public void testStartRegionBeforeFinish() throws IOException {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 1, dataChecksumEnabled);

        fileWriter.startRegion(false, StorageTestUtils.MAP_PARTITION_ID);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 5), false);
        fileWriter.startRegion(false, StorageTestUtils.MAP_PARTITION_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishWritingBeforeFinishRegion() throws Exception {
        LocalReducePartitionFileWriter fileWriter =
                new LocalReducePartitionFileWriter(createPartitionFile(), 1, dataChecksumEnabled);

        fileWriter.startRegion(false, StorageTestUtils.MAP_PARTITION_ID);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0), false);
        fileWriter.prepareFinishWriting(
                new BufferOrMarker.InputFinishedMarker(
                        StorageTestUtils.MAP_PARTITION_ID, new TestDataCommitListener()));
    }

    private LocalReducePartitionFile createPartitionFile() {
        return StorageTestUtils.createLocalReducePartitionFile(
                temporaryFolder.getRoot().getAbsolutePath());
    }
}
