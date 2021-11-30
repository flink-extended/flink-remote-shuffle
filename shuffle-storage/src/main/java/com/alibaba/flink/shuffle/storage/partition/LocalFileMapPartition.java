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
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.DataRegionCreditListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** A {@link DataPartition} implementation which writes data to and read data from local file. */
public class LocalFileMapPartition extends BaseMapPartition {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileMapPartition.class);

    /** {@link DataPartitionMeta} of this data partition. */
    private final LocalFileMapPartitionMeta partitionMeta;

    /** Local file storing all data of this data partition. */
    private final LocalMapPartitionFile partitionFile;

    public LocalFileMapPartition(
            StorageMeta storageMeta,
            PartitionedDataStore dataStore,
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID partitionID,
            int numReducePartitions) {
        super(dataStore, getSingleThreadExecutor(dataStore, storageMeta));

        String storagePath = storageMeta.getStoragePath();
        File storageDir = new File(storagePath);
        CommonUtils.checkArgument(storagePath.endsWith("/"), "Illegal storage path.");
        CommonUtils.checkArgument(storageDir.exists(), "Storage path does not exist.");
        CommonUtils.checkArgument(storageDir.isDirectory(), "Storage path is not a directory.");

        Configuration configuration = dataStore.getConfiguration();
        ConfigOption<Integer> configOption = StorageOptions.STORAGE_FILE_TOLERABLE_FAILURES;
        int tolerableFailures = CommonUtils.checkNotNull(configuration.getInteger(configOption));

        String fileName = CommonUtils.randomHexString(32);
        LocalMapPartitionFileMeta fileMeta =
                new LocalMapPartitionFileMeta(
                        storagePath + fileName,
                        numReducePartitions,
                        LocalMapPartitionFile.LATEST_STORAGE_VERSION);
        this.partitionFile = new LocalMapPartitionFile(fileMeta, tolerableFailures, true);
        this.partitionMeta =
                new LocalFileMapPartitionMeta(jobID, dataSetID, partitionID, fileMeta, storageMeta);
    }

    /**
     * Used to construct data partition instances when adding a finished external data partition or
     * recovering after failure.
     */
    public LocalFileMapPartition(
            PartitionedDataStore dataStore, LocalFileMapPartitionMeta partitionMeta) {
        super(dataStore, getSingleThreadExecutor(dataStore, partitionMeta.getStorageMeta()));

        this.partitionMeta = partitionMeta;
        LocalMapPartitionFileMeta fileMeta = partitionMeta.getPartitionFileMeta();
        this.partitionFile = fileMeta.createPersistentFile(dataStore.getConfiguration());

        if (!partitionFile.isConsumable()) {
            partitionFile.setConsumable(false);
            throw new ShuffleException("Partition data is not consumable.");
        }
    }

    private static SingleThreadExecutor getSingleThreadExecutor(
            PartitionedDataStore dataStore, StorageMeta storageMeta) {
        CommonUtils.checkArgument(dataStore != null, "Must be not null.");
        CommonUtils.checkArgument(storageMeta != null, "Must be not null.");

        return dataStore.getExecutorPool(storageMeta).getSingleThreadExecutor();
    }

    @Override
    public boolean isConsumable() {
        return partitionFile.isConsumable();
    }

    @Override
    protected DataPartitionReader getDataPartitionReader(
            int startPartitionIndex,
            int endPartitionIndex,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        // for different storage versions and formats, different file reader implementations are
        // needed for backward compatibility, we must keep backward compatibility when upgrading
        int storageVersion = partitionFile.getFileMeta().getStorageVersion();
        if (storageVersion <= 1) {
            boolean dataChecksumEnabled =
                    dataStore
                            .getConfiguration()
                            .getBoolean(StorageOptions.STORAGE_ENABLE_DATA_CHECKSUM);
            LocalMapPartitionFileReader fileReader =
                    new LocalMapPartitionFileReader(
                            dataChecksumEnabled,
                            startPartitionIndex,
                            endPartitionIndex,
                            partitionFile);
            return new LocalFileMapPartitionReader(
                    fileReader, dataListener, backlogListener, failureListener);
        }

        throw new ShuffleException(
                String.format(
                        "Illegal storage version, current: %d, supported: %d.",
                        storageVersion, partitionFile.getLatestStorageVersion()));
    }

    @Override
    protected DataPartitionWriter getDataPartitionWriter(
            MapPartitionID mapPartitionID,
            DataRegionCreditListener dataRegionCreditListener,
            FailureListener failureListener) {
        boolean dataChecksumEnabled =
                dataStore
                        .getConfiguration()
                        .getBoolean(StorageOptions.STORAGE_ENABLE_DATA_CHECKSUM);
        return new LocalFileMapPartitionWriter(
                dataChecksumEnabled,
                mapPartitionID,
                this,
                dataRegionCreditListener,
                failureListener,
                partitionFile);
    }

    @Override
    protected void releaseInternal(Throwable releaseCause) throws Exception {
        Throwable exception = null;

        try {
            super.releaseInternal(releaseCause);
        } catch (Throwable throwable) {
            exception = throwable;
            LOG.error("Fatal: failed to release base map partition.", throwable);
        }

        try {
            partitionFile.deleteFile();
        } catch (Throwable throwable) {
            exception = throwable;
            LOG.error("Fatal: failed to delete the partition file.", throwable);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    @Override
    public LocalFileMapPartitionMeta getPartitionMeta() {
        return partitionMeta;
    }
}
