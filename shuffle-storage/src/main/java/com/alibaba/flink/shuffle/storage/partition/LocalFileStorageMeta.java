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
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageType;
import com.alibaba.flink.shuffle.storage.utils.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/** {@link StorageMeta} for local file system. */
public class LocalFileStorageMeta extends StorageMeta {

    private static final long serialVersionUID = 6742470314776719614L;

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileStorageMeta.class);

    private static final String HEALTH_CHECK_FILE_SUFFIX = ".health_check";

    public LocalFileStorageMeta(String storagePath, StorageType storageType, String storageName) {
        super(storagePath, storageType, storageName);
    }

    @Override
    public long updateFreeStorageSpace() {
        numFreeSpaceBytes = Paths.get(storagePath).toFile().getUsableSpace();
        return numFreeSpaceBytes;
    }

    @Override
    public void updateStorageHealthStatus() {
        CommonUtils.checkState(storagePath.endsWith("/"), "Storage path must end with '/'.");
        Path path =
                new File(storagePath + CommonUtils.randomHexString(32) + HEALTH_CHECK_FILE_SUFFIX)
                        .toPath();
        FileChannel file = null;
        try {
            file = IOUtils.createWritableFileChannel(path);
            file.close();
            if (!Files.isWritable(path) || !Files.isReadable(path)) {
                throw new IOException("File is not readable/writable.");
            }
            Files.delete(path);
            isHealthy = true;
        } catch (Throwable throwable) {
            isHealthy = false;
            LOG.warn("Found unhealthy storage: {}.", this, throwable);

            if (file == null) {
                return;
            }
            CommonUtils.runQuietly(file::close);
            CommonUtils.runQuietly(() -> Files.deleteIfExists(path));
        }
    }
}
