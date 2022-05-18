/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

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

/** Tests for {@link LocalMapPartitionFileWriter}. */
@RunWith(Parameterized.class)
public class LocalMapPartitionFileWriterTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalMapPartitionFileWriterTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testWriteData() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
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
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                0,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                0,
                false,
                dataChecksumEnabled);

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);

        LocalMapPartitionFileMeta fileMeta = partitionFile.getFileMeta();
        assertEquals(0, new File(fileMeta.getDataFilePath().toString()).length());
        assertEquals(
                LocalMapPartitionFile.INDEX_DATA_CHECKSUM_SIZE,
                new File(fileMeta.getIndexFilePath().toString()).length());
    }

    @Test
    public void testWriteWithEmptyReducePartition() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
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
        LocalMapPartitionFile partitionFile = createPartitionFile();
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(partitionFile, 2, dataChecksumEnabled);
        fileWriter.open();

        fileWriter.startRegion(false);
        ByteBuffer data = ByteBuffer.allocateDirect(0);
        fileWriter.writeBuffer(StorageTestUtils.createDataBuffer(data, 0));
        fileWriter.finishRegion();
        fileWriter.finishWriting();

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);

        LocalMapPartitionFileMeta fileMeta = partitionFile.getFileMeta();
        assertEquals(0, new File(fileMeta.getDataFilePath().toString()).length());
        assertEquals(
                LocalMapPartitionFile.INDEX_DATA_CHECKSUM_SIZE,
                new File(fileMeta.getIndexFilePath().toString()).length());
    }

    @Test(expected = IllegalStateException.class)
    public void testStartRegionAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.startRegion(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishRegionAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.finishRegion();
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteBufferAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 1, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishWritingAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.close();
        fileWriter.finishWriting();
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteNotInPartitionIndexOrder() throws IOException {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2, dataChecksumEnabled);

        fileWriter.startRegion(false);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 5));
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testStartRegionBeforeFinish() throws IOException {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 1, dataChecksumEnabled);

        fileWriter.startRegion(false);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 5));
        fileWriter.startRegion(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishWritingBeforeFinishRegion() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 1, dataChecksumEnabled);

        fileWriter.startRegion(false);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0));
        fileWriter.finishWriting();
    }

    private LocalMapPartitionFile createPartitionFile() {
        return StorageTestUtils.createLocalMapPartitionFile(
                temporaryFolder.getRoot().getAbsolutePath());
    }
}
