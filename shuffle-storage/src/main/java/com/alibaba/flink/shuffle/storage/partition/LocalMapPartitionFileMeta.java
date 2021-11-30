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

/** {@link PersistentFileMeta} of {@link LocalMapPartitionFile}. */
public class LocalMapPartitionFileMeta implements PersistentFileMeta {

    private static final long serialVersionUID = -6682157834905760822L;

    /** File name (without suffix) of both the data file and index file. */
    private final String filePath;

    /** Number of reduce partitions in the corresponding partition file. */
    private final int numReducePartitions;

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

    public LocalMapPartitionFileMeta(String filePath, int numReducePartitions, int storageVersion) {
        CommonUtils.checkArgument(filePath != null, "Must be not null.");
        CommonUtils.checkArgument(numReducePartitions > 0, "Must be positive.");

        this.filePath = filePath;
        this.numReducePartitions = numReducePartitions;
        this.storageVersion = storageVersion;

        this.partialDataFilePath = LocalMapPartitionFile.getPartialDataFilePath(filePath);
        this.partialIndexFilePath = LocalMapPartitionFile.getPartialIndexFilePath(filePath);
        this.dataFilePath = LocalMapPartitionFile.getDataFilePath(filePath);
        this.indexFilePath = LocalMapPartitionFile.getIndexFilePath(filePath);
    }

    @Override
    public int getStorageVersion() {
        return storageVersion;
    }

    /**
     * Reconstructs the {@link LocalMapPartitionFileMeta} instance from the {@link DataInput} when
     * recovering from failure.
     */
    public static LocalMapPartitionFileMeta readFrom(DataInput dataInput) throws IOException {
        int storageVersion = dataInput.readInt();
        return readFrom(dataInput, storageVersion);
    }

    private static LocalMapPartitionFileMeta readFrom(DataInput dataInput, int storageVersion)
            throws IOException {
        if (storageVersion == 0) {
            int numReducePartitions = dataInput.readInt();
            String filePath = dataInput.readUTF();
            return new LocalMapPartitionFileMeta(filePath, numReducePartitions, storageVersion);
        }

        throw new ShuffleException(
                String.format(
                        "Illegal storage version, data format version: %d, supported version: %d.",
                        storageVersion, LocalMapPartitionFile.LATEST_STORAGE_VERSION));
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

    public int getNumReducePartitions() {
        return numReducePartitions;
    }

    @Override
    public LocalMapPartitionFile createPersistentFile(Configuration configuration) {
        ConfigOption<Integer> configOption = StorageOptions.STORAGE_FILE_TOLERABLE_FAILURES;
        int tolerableFailures = CommonUtils.checkNotNull(configuration.getInteger(configOption));

        LocalMapPartitionFile partitionFile =
                new LocalMapPartitionFile(this, tolerableFailures, false);
        partitionFile.setConsumable(true);
        return partitionFile;
    }

    @Override
    public void writeTo(DataOutput dataOutput) throws Exception {
        dataOutput.writeInt(storageVersion);
        dataOutput.writeInt(numReducePartitions);
        dataOutput.writeUTF(filePath);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (!(that instanceof LocalMapPartitionFileMeta)) {
            return false;
        }

        LocalMapPartitionFileMeta fileMeta = (LocalMapPartitionFileMeta) that;
        return numReducePartitions == fileMeta.numReducePartitions
                && storageVersion == fileMeta.storageVersion
                && Objects.equals(filePath, fileMeta.filePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filePath, numReducePartitions, storageVersion);
    }

    @Override
    public String toString() {
        return "LocalMapPartitionFileMeta{"
                + "FilePath="
                + filePath
                + ", NumReducePartitions="
                + numReducePartitions
                + ", StorageVersion="
                + storageVersion
                + '}';
    }
}
