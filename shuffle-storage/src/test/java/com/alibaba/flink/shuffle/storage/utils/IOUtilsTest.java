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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/** Tests for {@link IOUtils}. */
public class IOUtilsTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testWriteBuffers() throws Exception {
        int numBuffers = 4000;
        int bufferSize = 4096;
        Random random = new Random();
        File file = temporaryFolder.newFile();

        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE)) {
            long totalBytes = 0;
            ByteBuffer[] buffers = new ByteBuffer[numBuffers];
            for (int i = 0; i < numBuffers; ++i) {
                ByteBuffer buffer =
                        CommonUtils.allocateDirectByteBuffer(random.nextInt(bufferSize) + 1);
                buffer.put(StorageTestUtils.DATA_BYTES, 0, buffer.capacity());
                buffer.flip();
                buffers[i] = buffer;
                totalBytes += buffer.capacity();
            }

            IOUtils.writeBuffers(fileChannel, buffers);
            assertEquals(totalBytes, fileChannel.size());
        }
    }
}
