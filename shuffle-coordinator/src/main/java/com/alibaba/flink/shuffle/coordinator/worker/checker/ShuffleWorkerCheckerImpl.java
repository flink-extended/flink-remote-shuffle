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

package com.alibaba.flink.shuffle.coordinator.worker.checker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.utils.WorkerCheckerUtils;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorker;
import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.storage.StorageMetrics;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.core.config.StorageOptions.STORAGE_CHECK_UPDATE_PERIOD;

/**
 * The implementation class of {@link ShuffleWorkerChecker} is used to obtain the status of the
 * {@link ShuffleWorker}, including storage status, data partition file information, etc.
 */
public class ShuffleWorkerCheckerImpl implements ShuffleWorkerChecker {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleWorkerCheckerImpl.class);

    private final ScheduledExecutorService storageCheckerService;

    private final PartitionedDataStore dataStore;

    /** Local storage space information of this shuffle worker. */
    private volatile StorageSpaceInfo storageSpaceInfo = StorageSpaceInfo.ZERO_STORAGE_SPACE;

    /**
     * The total bytes of all data partition in the data store, including total bytes of data files
     * and index files.
     */
    private volatile long numTotalPartitionFileBytes;

    public long getNumTotalPartitionFileBytes() {
        return numTotalPartitionFileBytes;
    }

    @Override
    public StorageSpaceInfo getStorageSpaceInfo() {
        return storageSpaceInfo;
    }

    @Override
    public void close() {
        if (storageCheckerService != null) {
            storageCheckerService.shutdownNow();
        }
    }

    public ShuffleWorkerCheckerImpl(Configuration configuration, PartitionedDataStore dataStore) {
        this.dataStore = checkNotNull(dataStore);

        this.storageCheckerService =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory("ShuffleWorker-Checker"));
        startStorageCheckerThread(configuration);
    }

    private void startStorageCheckerThread(Configuration configuration) {
        try {
            long storageCheckerPeriod =
                    configuration.getDuration(STORAGE_CHECK_UPDATE_PERIOD).toMillis();
            checkArgument(
                    storageCheckerPeriod > 0L, "The storage check interval must be larger than 0.");

            StorageCheckRunnable storageCheckRunnable = new StorageCheckRunnable();
            storageCheckRunnable.run();

            StorageMetrics.registerGaugeForNumHddMaxFreeBytes(
                    storageSpaceInfo::getHddMaxFreeSpaceBytes);
            StorageMetrics.registerGaugeForNumSsdMaxFreeBytes(
                    storageSpaceInfo::getSsdMaxFreeSpaceBytes);
            StorageMetrics.registerGaugeForNumHddMaxUsedBytes(
                    storageSpaceInfo::getHddMaxUsedSpaceBytes);
            StorageMetrics.registerGaugeForNumSsdMaxUsedBytes(
                    storageSpaceInfo::getSsdMaxUsedSpaceBytes);
            StorageMetrics.registerGaugeForNumTotalPartitionFileBytes(
                    this::getNumTotalPartitionFileBytes);

            storageCheckerService.scheduleAtFixedRate(
                    storageCheckRunnable,
                    storageCheckerPeriod,
                    storageCheckerPeriod,
                    TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to start storage check thread", t);
        }
    }

    private class StorageCheckRunnable implements Runnable {
        @Override
        public void run() {
            long start = System.nanoTime();

            try {
                numTotalPartitionFileBytes = dataStore.updateUsedStorageSpace();
            } catch (Throwable t) {
                LOG.error("Failed to update the used space.", t);
            }

            try {
                dataStore.updateStorageHealthStatus();
            } catch (Throwable t) {
                LOG.error("Failed to update the health status.", t);
            }

            try {
                dataStore.updateFreeStorageSpace();
            } catch (Throwable t) {
                LOG.error("Failed to update the free space.", t);
            }

            try {
                StorageSpaceInfo newStorageSpaceInfo =
                        dataStore
                                .getStorageSpaceInfos()
                                .get(LocalFileMapPartitionFactory.class.getName());
                if (newStorageSpaceInfo != null) {
                    storageSpaceInfo = newStorageSpaceInfo;
                }
            } catch (Throwable t) {
                LOG.error("Failed to update the storage space information.", t);
            }

            LOG.info(
                    "Update data store info in {} ms, max free space, HDD: {}({}), SSD:{}({}), "
                            + "max used space, HDD: {}({}), SSD:{}({}), "
                            + "total partition file bytes: {}({}).",
                    String.format("%.2f", (float) ((System.nanoTime() - start) / 1000000)),
                    WorkerCheckerUtils.bytesToHumanReadable(
                            storageSpaceInfo.getHddMaxFreeSpaceBytes()),
                    storageSpaceInfo.getHddMaxFreeSpaceBytes(),
                    WorkerCheckerUtils.bytesToHumanReadable(
                            storageSpaceInfo.getSsdMaxFreeSpaceBytes()),
                    storageSpaceInfo.getSsdMaxFreeSpaceBytes(),
                    WorkerCheckerUtils.bytesToHumanReadable(
                            storageSpaceInfo.getHddMaxUsedSpaceBytes()),
                    storageSpaceInfo.getHddMaxUsedSpaceBytes(),
                    WorkerCheckerUtils.bytesToHumanReadable(
                            storageSpaceInfo.getSsdMaxUsedSpaceBytes()),
                    storageSpaceInfo.getSsdMaxUsedSpaceBytes(),
                    WorkerCheckerUtils.bytesToHumanReadable(numTotalPartitionFileBytes),
                    numTotalPartitionFileBytes);
        }
    }
}
