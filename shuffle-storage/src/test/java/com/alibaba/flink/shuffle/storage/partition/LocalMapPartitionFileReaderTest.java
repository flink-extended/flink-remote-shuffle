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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.storage.exception.FileCorruptedException;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/** Tests for {@link LocalMapPartitionFileReader}. */
@RunWith(Parameterized.class)
public class LocalMapPartitionFileReaderTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalMapPartitionFileReaderTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testReadData() throws Exception {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                false,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 1);
        assertEquals(numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadEmptyData() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile, 0, 1, 1, false, dataChecksumEnabled);

        LocalMapPartitionFileReader fileReader =
                new LocalMapPartitionFileReader(dataChecksumEnabled, 0, 0, partitionFile);
        assertFalse(fileReader.hasRemaining());
        fileReader.finishReading();
    }

    @Test
    public void testReadWithEmptyReducePartitions() throws Exception {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                true,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 1);
        assertEquals(
                numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS / 2, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadMultipleReducePartitions() throws Exception {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                false,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 3);
        assertEquals(numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadMultipleReducePartitionsWithBroadcastRegion() throws Exception {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                false,
                5,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 3);
        assertEquals(numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadWithBroadcastRegion() throws Exception {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                false,
                5,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 1);
        assertEquals(numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testReadMultipleReducePartitionsWithEmptyOnes() throws Exception {
        int numRegions = 10;
        int numBuffers = 100;
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                numRegions,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                numBuffers,
                true,
                dataChecksumEnabled);

        int buffersRead = readData(partitionFile, 3);
        assertEquals(
                numRegions * numBuffers * StorageTestUtils.NUM_REDUCE_PARTITIONS / 2, buffersRead);
        assertNull(partitionFile.getIndexReadingChannel());
        assertNull(partitionFile.getDataReadingChannel());
    }

    @Test
    public void testIndexFileCorruptedWithIncompleteRegion() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                10,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                10,
                false,
                dataChecksumEnabled);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.INDEX_FILE_SUFFIX)) {
                try (FileChannel fileChannel =
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
                    fileChannel.truncate(10);
                }
            }
        }

        LocalMapPartitionFileReader fileReader =
                new LocalMapPartitionFileReader(dataChecksumEnabled, 0, 0, partitionFile);
        assertThrows(FileCorruptedException.class, fileReader::open);
        assertFalse(partitionFile.isConsumable());
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testIndexFileCorruptedWithWrongChecksum() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                10,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                10,
                false,
                dataChecksumEnabled);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.INDEX_FILE_SUFFIX)) {
                try (FileChannel fileChannel =
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
                    int indexRegionSize =
                            LocalMapPartitionFile.INDEX_ENTRY_SIZE
                                    * StorageTestUtils.NUM_REDUCE_PARTITIONS;
                    fileChannel.truncate(fileChannel.size() - indexRegionSize);
                }
            }
        }

        LocalMapPartitionFileReader fileReader =
                new LocalMapPartitionFileReader(dataChecksumEnabled, 0, 0, partitionFile);
        assertThrows(FileCorruptedException.class, fileReader::open);
        assertFalse(partitionFile.isConsumable());
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testDataFileCorrupted() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile,
                10,
                StorageTestUtils.NUM_REDUCE_PARTITIONS,
                10,
                false,
                dataChecksumEnabled);

        for (File file : CommonUtils.checkNotNull(temporaryFolder.getRoot().listFiles())) {
            if (file.getPath().contains(LocalMapPartitionFile.DATA_FILE_SUFFIX)) {
                try (FileChannel fileChannel =
                        FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
                    fileChannel.truncate(10);
                }
            }
        }

        LocalMapPartitionFileReader fileReader =
                new LocalMapPartitionFileReader(dataChecksumEnabled, 0, 0, partitionFile);
        assertThrows(FileCorruptedException.class, fileReader::open);
        assertFalse(partitionFile.isConsumable());
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    private LocalMapPartitionFile createPartitionFile() {
        return StorageTestUtils.createLocalMapPartitionFile(
                temporaryFolder.getRoot().getAbsolutePath());
    }

    private int readData(LocalMapPartitionFile partitionFile, int numPartitions) throws Exception {
        Queue<LocalMapPartitionFileReader> fileReaders = new ArrayDeque<>();
        for (int partitionIndex = 0; partitionIndex < StorageTestUtils.NUM_REDUCE_PARTITIONS; ) {
            LocalMapPartitionFileReader fileReader =
                    new LocalMapPartitionFileReader(
                            dataChecksumEnabled,
                            partitionIndex,
                            Math.min(
                                    partitionIndex + numPartitions - 1,
                                    StorageTestUtils.NUM_REDUCE_PARTITIONS - 1),
                            partitionFile);
            fileReader.open();
            fileReaders.add(fileReader);
            partitionIndex += numPartitions;
        }

        int buffersRead = 0;
        ByteBuffer buffer = ByteBuffer.allocate(StorageTestUtils.DATA_BUFFER_SIZE);
        while (!fileReaders.isEmpty()) {
            LocalMapPartitionFileReader fileReader = fileReaders.poll();
            if (!fileReader.hasRemaining()) {
                fileReader.finishReading();
                continue;
            }
            fileReaders.add(fileReader);

            buffer.clear();
            fileReader.readBuffer(buffer);
            ++buffersRead;
            assertEquals(ByteBuffer.wrap(StorageTestUtils.DATA_BYTES), buffer);
        }
        return buffersRead;
    }
}
