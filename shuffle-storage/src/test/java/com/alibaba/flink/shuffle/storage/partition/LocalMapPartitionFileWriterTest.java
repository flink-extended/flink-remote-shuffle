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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalMapPartitionFileWriter}. */
public class LocalMapPartitionFileWriterTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWriteData() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile, 10, StorageTestUtils.NUM_REDUCE_PARTITIONS, 10, false);

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testWriteEmptyData() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile, 0, StorageTestUtils.NUM_REDUCE_PARTITIONS, 0, false);

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
                partitionFile, 10, StorageTestUtils.NUM_REDUCE_PARTITIONS, 10, true);

        assertTrue(partitionFile.isConsumable());
        assertEquals(2, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testWriteEmptyBuffer() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        LocalMapPartitionFileWriter fileWriter = new LocalMapPartitionFileWriter(partitionFile, 2);
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
                new LocalMapPartitionFileWriter(createPartitionFile(), 2);

        fileWriter.close();
        fileWriter.startRegion(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishRegionAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2);

        fileWriter.close();
        fileWriter.finishRegion();
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteBufferAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 1);

        fileWriter.close();
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishWritingAfterClose() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2);

        fileWriter.close();
        fileWriter.finishWriting();
    }

    @Test(expected = IllegalStateException.class)
    public void testWriteNotInPartitionIndexOrder() throws IOException {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 2);

        fileWriter.startRegion(false);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 5));
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 0));
    }

    @Test(expected = IllegalStateException.class)
    public void testStartRegionBeforeFinish() throws IOException {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 1);

        fileWriter.startRegion(false);
        fileWriter.writeBuffer(
                StorageTestUtils.createDataBuffer(StorageTestUtils.createRandomData(), 5));
        fileWriter.startRegion(false);
    }

    @Test(expected = IllegalStateException.class)
    public void testFinishWritingBeforeFinishRegion() throws Exception {
        LocalMapPartitionFileWriter fileWriter =
                new LocalMapPartitionFileWriter(createPartitionFile(), 1);

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
