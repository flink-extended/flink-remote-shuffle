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

import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.StorageOptions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

/** {@link PersistentFileMeta} of {@link LocalReducePartitionFile}. */
public class LocalReducePartitionFileMeta implements PersistentFileMeta {
    private static final long serialVersionUID = -3499083473292011948L;

    /** File name (without suffix) of both the data file and index file. */
    private final String filePath;

    /** Number of map partitions in the corresponding partition file. */
    private final int numMapPartitions;

    /** Path of the data file. */
    private final Path dataFilePath;

    /** Path of the index file. */
    private final Path indexFilePath;

    /** Path of the in-progressing partial data file. */
    private final Path partialDataFilePath;

    /** Path of the in-progressing partial index file. */
    private final Path partialIndexFilePath;

    /**
     * Storage version of the target {@link PersistentFile}. Different versions may need different
     * processing logics.
     */
    private final int storageVersion;

    public LocalReducePartitionFileMeta(String filePath, int numMapPartitions, int storageVersion) {
        CommonUtils.checkArgument(filePath != null, "Must be not null.");
        CommonUtils.checkArgument(numMapPartitions > 0, "Must be positive.");

        this.filePath = filePath;
        this.numMapPartitions = numMapPartitions;
        this.storageVersion = storageVersion;

        this.partialDataFilePath = LocalReducePartitionFile.getPartialDataFilePath(filePath);
        this.partialIndexFilePath = LocalReducePartitionFile.getPartialIndexFilePath(filePath);
        this.dataFilePath = LocalReducePartitionFile.getDataFilePath(filePath);
        this.indexFilePath = LocalReducePartitionFile.getIndexFilePath(filePath);
    }

    @Override
    public int getStorageVersion() {
        return storageVersion;
    }

    /**
     * Reconstructs the {@link LocalReducePartitionFileMeta} instance from the {@link DataInput}
     * when recovering from failure.
     */
    public static LocalReducePartitionFileMeta readFrom(DataInput dataInput) throws IOException {
        int storageVersion = dataInput.readInt();
        return readFrom(dataInput, storageVersion);
    }

    private static LocalReducePartitionFileMeta readFrom(DataInput dataInput, int storageVersion)
            throws IOException {
        if (storageVersion <= 1) {
            int numReducePartitions = dataInput.readInt();
            String filePath = dataInput.readUTF();
            return new LocalReducePartitionFileMeta(filePath, numReducePartitions, storageVersion);
        }

        throw new ShuffleException(
                String.format(
                        "Illegal storage version, data format version: %d, supported version: %d.",
                        storageVersion, LocalReducePartitionFile.LATEST_STORAGE_VERSION));
    }

    public String getFilePath() {
        return filePath;
    }

    public Path getDataFilePath() {
        return dataFilePath;
    }

    public Path getIndexFilePath() {
        return indexFilePath;
    }

    public Path getPartialDataFilePath() {
        return partialDataFilePath;
    }

    public Path getPartialIndexFilePath() {
        return partialIndexFilePath;
    }

    public int getNumMapPartitions() {
        return numMapPartitions;
    }

    @Override
    public LocalReducePartitionFile createPersistentFile(Configuration configuration) {
        ConfigOption<Integer> configOption = StorageOptions.STORAGE_FILE_TOLERABLE_FAILURES;
        int tolerableFailures = CommonUtils.checkNotNull(configuration.getInteger(configOption));

        LocalReducePartitionFile partitionFile =
                new LocalReducePartitionFile(this, tolerableFailures, false);
        partitionFile.setConsumable(true);
        return partitionFile;
    }

    @Override
    public void writeTo(DataOutput dataOutput) throws Exception {
        dataOutput.writeInt(storageVersion);
        dataOutput.writeInt(numMapPartitions);
        dataOutput.writeUTF(filePath);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (!(that instanceof LocalReducePartitionFileMeta)) {
            return false;
        }

        LocalReducePartitionFileMeta fileMeta = (LocalReducePartitionFileMeta) that;
        return numMapPartitions == fileMeta.numMapPartitions
                && storageVersion == fileMeta.storageVersion
                && Objects.equals(filePath, fileMeta.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, numMapPartitions, storageVersion);
    }

    @Override
    public String toString() {
        return "LocalReducePartitionFileMeta{"
                + "FilePath="
                + filePath
                + ", NumReducePartitions="
                + numMapPartitions
                + ", StorageVersion="
                + storageVersion
                + '}';
    }
}
