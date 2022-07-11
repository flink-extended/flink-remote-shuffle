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

import com.alibaba.flink.shuffle.storage.utils.StorageTestUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** Tests for {@link LocalReducePartitionFileMeta}. */
public class LocalReducePartitionFileMetaTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testSerializeAndDeserialize() throws Exception {
        LocalReducePartitionFileMeta fileMeta =
                StorageTestUtils.createLocalReducePartitionFileMeta();

        File tmpFile = temporaryFolder.newFile();
        try (DataOutputStream output = new DataOutputStream(new FileOutputStream(tmpFile))) {
            fileMeta.writeTo(output);
        }

        LocalReducePartitionFileMeta recovered;
        try (DataInputStream input = new DataInputStream(new FileInputStream(tmpFile))) {
            recovered = LocalReducePartitionFileMeta.readFrom(input);
        }

        assertEquals(fileMeta, recovered);
    }

    @Test
    public void testIllegalArgument() {
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new LocalReducePartitionFileMeta(
                                null, 10, LocalMapPartitionFile.LATEST_STORAGE_VERSION));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        new LocalReducePartitionFileMeta(
                                "/tmp/test", 0, LocalMapPartitionFile.LATEST_STORAGE_VERSION));
    }
}
