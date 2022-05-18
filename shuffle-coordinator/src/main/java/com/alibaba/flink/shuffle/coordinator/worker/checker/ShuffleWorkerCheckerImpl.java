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

package com.alibaba.flink.shuffle.coordinator.worker.checker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorker;
import com.alibaba.flink.shuffle.core.executor.ExecutorThreadFactory;
import com.alibaba.flink.shuffle.core.storage.DataStoreStatistics;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.coordinator.utils.WorkerCheckerUtils.bytesToHumanReadable;
import static com.alibaba.flink.shuffle.core.config.StorageOptions.STORAGE_CHECK_UPDATE_PERIOD;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerAgvDataFileBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerAvgIndexFileBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerHddMaxFreeBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerHddMaxUsedBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerMaxDataFileBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerMaxIndexFileBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerMaxNumDataRegions;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerNumDataPartitions;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerSsdMaxFreeBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerSsdMaxUsedBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerTotalDataFileBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerTotalIndexFileBytes;
import static com.alibaba.flink.shuffle.storage.StorageMetricsUtil.registerTotalPartitionFileBytes;

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

    /** Statistics information of the target {@link PartitionedDataStore}. */
    private volatile DataStoreStatistics dataStoreStatistics =
            DataStoreStatistics.EMPTY_DATA_STORE_STATISTICS;

    @Override
    public DataStoreStatistics getDataStoreStatistics() {
        return dataStoreStatistics;
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

            registerHddMaxFreeBytes(() -> getStorageSpaceInfo().getHddMaxFreeSpaceBytes());
            registerSsdMaxFreeBytes(() -> getStorageSpaceInfo().getSsdMaxFreeSpaceBytes());
            registerHddMaxUsedBytes(() -> getStorageSpaceInfo().getHddMaxUsedSpaceBytes());
            registerSsdMaxUsedBytes(() -> getStorageSpaceInfo().getSsdMaxUsedSpaceBytes());
            registerNumDataPartitions(() -> getDataStoreStatistics().getNumDataPartitions());
            registerMaxNumDataRegions(() -> getDataStoreStatistics().getMaxNumDataRegions());
            registerMaxIndexFileBytes(() -> getDataStoreStatistics().getMaxIndexFileBytes());
            registerMaxDataFileBytes(() -> getDataStoreStatistics().getMaxDataFileBytes());
            registerAvgIndexFileBytes(() -> getDataStoreStatistics().getAvgIndexFileBytes());
            registerAgvDataFileBytes(() -> getDataStoreStatistics().getAvgDataFileBytes());
            registerTotalIndexFileBytes(() -> getDataStoreStatistics().getTotalIndexFileBytes());
            registerTotalDataFileBytes(() -> getDataStoreStatistics().getTotalDataFileBytes());
            registerTotalPartitionFileBytes(
                    () -> getDataStoreStatistics().getTotalPartitionFileBytes());

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
                dataStore.updateUsedStorageSpace();
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

            try {
                // Currently, the statistics is updated per checking period. In the future, we can
                // switch to metric reporting period.
                dataStoreStatistics = dataStore.getDataStoreStatistics();
            } catch (Throwable t) {
                LOG.error("Failed to update the data store statistics.", t);
            }

            LOG.info(
                    "Update data store info in {} ms, max free space, HDD: {}({}), SSD:{}({}), "
                            + "max used space, HDD: {}({}), SSD:{}({}), "
                            + "total partition file bytes: {}({}).",
                    String.format("%.2f", (float) ((System.nanoTime() - start) / 1000000)),
                    bytesToHumanReadable(storageSpaceInfo.getHddMaxFreeSpaceBytes()),
                    storageSpaceInfo.getHddMaxFreeSpaceBytes(),
                    bytesToHumanReadable(storageSpaceInfo.getSsdMaxFreeSpaceBytes()),
                    storageSpaceInfo.getSsdMaxFreeSpaceBytes(),
                    bytesToHumanReadable(storageSpaceInfo.getHddMaxUsedSpaceBytes()),
                    storageSpaceInfo.getHddMaxUsedSpaceBytes(),
                    bytesToHumanReadable(storageSpaceInfo.getSsdMaxUsedSpaceBytes()),
                    storageSpaceInfo.getSsdMaxUsedSpaceBytes(),
                    bytesToHumanReadable(dataStoreStatistics.getTotalPartitionFileBytes()),
                    dataStoreStatistics.getTotalPartitionFileBytes());
        }
    }
}
