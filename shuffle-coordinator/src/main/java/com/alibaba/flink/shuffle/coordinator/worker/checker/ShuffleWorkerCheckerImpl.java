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
import com.alibaba.flink.shuffle.core.storage.UsableStorageSpaceInfo;
import com.alibaba.flink.shuffle.storage.StorageMetrics;
import com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    /** The max storage usable bytes of all HDD storage disks. */
    private final AtomicLong numHddMaxUsableBytes = new AtomicLong(-1);

    /** The max storage usable bytes of all SSD storage disks. */
    private final AtomicLong numSsdMaxUsableBytes = new AtomicLong(-1);

    /**
     * The total bytes of all data partition in the data store, including total bytes of data files
     * and index files.
     */
    private long numTotalPartitionFileBytes;

    public long getNumTotalPartitionFileBytes() {
        return numTotalPartitionFileBytes;
    }

    @Override
    public long getNumHddMaxUsableBytes() {
        return numHddMaxUsableBytes.get();
    }

    @Override
    public long getNumSsdMaxUsableBytes() {
        return numSsdMaxUsableBytes.get();
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

            StorageMetrics.registerGaugeForNumHddMaxUsableBytes(this::getNumHddMaxUsableBytes);
            StorageMetrics.registerGaugeForNumSsdMaxUsableBytes(this::getNumSsdMaxUsableBytes);
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
            try {
                updateStorageUsableSpaces();
            } catch (Throwable t) {
                LOG.error("Failed to update the usable spaces,", t);
            }
        }

        private void updateStorageUsableSpaces() {
            long start = System.nanoTime();
            updateUsableStorageSpace();
            numTotalPartitionFileBytes = dataStore.numDataPartitionTotalBytes();
            LOG.info(
                    "Update data store info in {} ms, max usable space, HDD: {}({}), SSD:{}({}), "
                            + "total partition file bytes: {}({}).",
                    String.format("%.2f", (float) ((System.nanoTime() - start) / 1000000)),
                    WorkerCheckerUtils.bytesToHumanReadable(numHddMaxUsableBytes.get()),
                    numHddMaxUsableBytes,
                    WorkerCheckerUtils.bytesToHumanReadable(numSsdMaxUsableBytes.get()),
                    numSsdMaxUsableBytes,
                    WorkerCheckerUtils.bytesToHumanReadable(numTotalPartitionFileBytes),
                    numTotalPartitionFileBytes);
        }

        private void updateUsableStorageSpace() {
            dataStore.updateUsableStorageSpace();
            UsableStorageSpaceInfo usableSpace =
                    dataStore
                            .getUsableStorageSpace()
                            .get(LocalFileMapPartitionFactory.class.getName());
            if (usableSpace != null) {
                numHddMaxUsableBytes.set(usableSpace.getHddUsableSpaceBytes());
                numSsdMaxUsableBytes.set(usableSpace.getSsdUsableSpaceBytes());
            }
        }
    }
}
