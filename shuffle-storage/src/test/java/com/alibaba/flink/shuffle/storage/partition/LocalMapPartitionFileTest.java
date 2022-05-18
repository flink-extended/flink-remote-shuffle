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
import com.alibaba.flink.shuffle.storage.exception.FileCorruptedException;
import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link LocalMapPartitionFile}. */
@RunWith(Parameterized.class)
public class LocalMapPartitionFileTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final boolean dataChecksumEnabled;

    @Parameterized.Parameters
    public static Object[] data() {
        return new Boolean[] {true, false};
    }

    public LocalMapPartitionFileTest(boolean dataChecksumEnabled) {
        this.dataChecksumEnabled = dataChecksumEnabled;
    }

    @Test
    public void testConsumable() throws Exception {
        LocalMapPartitionFile partitionFile1 = createPartitionFile();
        assertTrue(partitionFile1.isConsumable());
        Files.delete(partitionFile1.getFileMeta().getDataFilePath());
        assertFalse(partitionFile1.isConsumable());
        partitionFile1.deleteFile();

        LocalMapPartitionFile partitionFile2 = createPartitionFile();
        assertTrue(partitionFile2.isConsumable());
        Files.delete(partitionFile2.getFileMeta().getIndexFilePath());
        assertFalse(partitionFile2.isConsumable());
        partitionFile2.deleteFile();

        LocalMapPartitionFile partitionFile3 = createPartitionFile();
        assertTrue(partitionFile3.isConsumable());
        partitionFile3.setConsumable(false);
        assertFalse(partitionFile3.isConsumable());
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testOpenAndCloseFile() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();

        Object reader1 = new Object();
        partitionFile.openFile(reader1);

        FileChannel dataChannel = CommonUtils.checkNotNull(partitionFile.getDataReadingChannel());
        FileChannel indexChannel = CommonUtils.checkNotNull(partitionFile.getIndexReadingChannel());
        assertTrue(dataChannel.isOpen());
        assertTrue(indexChannel.isOpen());

        Object reader2 = new Object();
        partitionFile.openFile(reader2);

        assertTrue(dataChannel.isOpen());
        assertTrue(indexChannel.isOpen());

        partitionFile.closeFile(reader1);

        assertTrue(dataChannel.isOpen());
        assertTrue(indexChannel.isOpen());

        partitionFile.closeFile(reader2);

        assertFalse(dataChannel.isOpen());
        assertFalse(indexChannel.isOpen());
        assertNull(partitionFile.getDataReadingChannel());
        assertNull(partitionFile.getIndexReadingChannel());
    }

    @Test
    public void testDeleteFile() throws Exception {
        LocalMapPartitionFile partitionFile = createPartitionFile();

        Object reader = new Object();
        partitionFile.openFile(reader);

        partitionFile.deleteFile();

        assertNull(partitionFile.getDataReadingChannel());
        assertNull(partitionFile.getIndexReadingChannel());
        assertEquals(0, CommonUtils.checkNotNull(temporaryFolder.getRoot().list()).length);
    }

    @Test
    public void testOnError() throws Exception {
        LocalMapPartitionFile partitionFile1 = createPartitionFile();
        partitionFile1.onError(new Exception("Test exception."));
        partitionFile1.onError(new Exception("Test exception."));
        partitionFile1.onError(new Exception("Test exception."));
        partitionFile1.onError(new Exception("Test exception."));
        assertTrue(partitionFile1.isConsumable());

        partitionFile1.onError(new FileCorruptedException());
        assertFalse(partitionFile1.isConsumable());

        LocalMapPartitionFile partitionFile2 = createPartitionFile();
        partitionFile2.onError(new ClosedChannelException());
        partitionFile2.onError(new ClosedChannelException());
        partitionFile2.onError(new ClosedChannelException());
        partitionFile2.onError(new ClosedChannelException());
        assertTrue(partitionFile2.isConsumable());

        assertTrue(partitionFile2.isConsumable());
        partitionFile2.onError(new IOException("Test exception."));

        assertTrue(partitionFile2.isConsumable());
        partitionFile2.onError(new IOException("Test exception."));

        assertTrue(partitionFile2.isConsumable());
        partitionFile2.onError(new IOException("Test exception."));

        assertTrue(partitionFile2.isConsumable());
        partitionFile2.onError(new IOException("Test exception."));
        assertFalse(partitionFile2.isConsumable());
    }

    private LocalMapPartitionFile createPartitionFile() throws Exception {
        String baseDir = temporaryFolder.getRoot().getAbsolutePath();
        LocalMapPartitionFile partitionFile = StorageTestUtils.createLocalMapPartitionFile(baseDir);
        StorageTestUtils.writeLocalMapPartitionFile(
                partitionFile, 1, 1, 1, false, dataChecksumEnabled);
        return partitionFile;
    }
}
