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

package com.alibaba.flink.shuffle.storage.datastore;

import com.alibaba.flink.shuffle.common.config.ConfigOption;
import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.exception.DuplicatedPartitionException;
import com.alibaba.flink.shuffle.core.exception.PartitionNotFoundException;
import com.alibaba.flink.shuffle.core.executor.SimpleSingleThreadExecutorPool;
import com.alibaba.flink.shuffle.core.executor.SingleThreadExecutorPool;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.listener.PartitionStateListener;
import com.alibaba.flink.shuffle.core.memory.BufferDispatcher;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;
import com.alibaba.flink.shuffle.core.storage.DataPartitionStatistics;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.DataSet;
import com.alibaba.flink.shuffle.core.storage.DataStoreStatistics;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.ReadingViewContext;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.storage.StorageMetricsUtil;
import com.alibaba.flink.shuffle.storage.utils.DataPartitionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Implementation of {@link PartitionedDataStore}. */
public class PartitionedDataStoreImpl implements PartitionedDataStore {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedDataStoreImpl.class);

    /** Lock object used to protect unsafe structures. */
    private final Object lock = new Object();

    /** Read-only configuration of the shuffle cluster. */
    private final Configuration configuration;

    /**
     * Listener to be notified when the state (created, deleted) of {@link DataPartition} changes.
     */
    private final PartitionStateListener partitionStateListener;

    /** All {@link DataSet}s in this {@link PartitionedDataStore} indexed by {@link JobID}. */
    @GuardedBy("lock")
    private final Map<JobID, Set<DataSetID>> dataSetsByJob;

    /** All {@link DataSet}s in this {@link PartitionedDataStore} indexed by {@link DataSetID}. */
    @GuardedBy("lock")
    private final Map<DataSetID, DataSet> dataSets;

    /** All available {@link DataPartitionFactory}s in the classpath. */
    private final Map<String, DataPartitionFactory> partitionFactories = new HashMap<>();

    /** Buffer pool from where to allocate buffers for data writing. */
    private final BufferDispatcher writingBufferDispatcher;

    /** Buffer pool from where to allocate buffers for data reading. */
    private final BufferDispatcher readingBufferDispatcher;

    /**
     * Executor pool from where to allocate single thread executors for data partition event
     * processing.
     */
    private final Map<StorageMeta, SingleThreadExecutorPool> executorPools =
            new ConcurrentHashMap<>();

    /** Whether this data store has been shut down or not. */
    @GuardedBy("lock")
    private boolean isShutDown;

    public PartitionedDataStoreImpl(
            Configuration configuration, PartitionStateListener partitionStateListener) {
        CommonUtils.checkArgument(configuration != null, "Must be not null.");
        CommonUtils.checkArgument(partitionStateListener != null, "Must be not null.");

        this.configuration = configuration;
        this.partitionStateListener = partitionStateListener;
        this.dataSets = new HashMap<>();
        this.dataSetsByJob = new HashMap<>();

        this.writingBufferDispatcher = createWritingBufferDispatcher(configuration);
        this.readingBufferDispatcher = createReadingBufferDispatcher(configuration);

        ServiceLoader<DataPartitionFactory> serviceLoader =
                ServiceLoader.load(DataPartitionFactory.class);

        synchronized (lock) {
            for (DataPartitionFactory partitionFactory : serviceLoader) {
                try {
                    partitionFactory.initialize(configuration);
                    partitionFactories.put(partitionFactory.getClass().getName(), partitionFactory);
                } catch (Throwable throwable) {
                    String className = partitionFactory.getClass().getName();
                    LOG.warn(
                            "Failed to initialize {} because '{}'.",
                            className,
                            throwable.getMessage());
                }
            }

            CommonUtils.checkState(
                    !partitionFactories.isEmpty(), "No valid partition factory found.");
        }
        StorageMetricsUtil.registerTotalNumExecutors(this::numTotalExecutors);
    }

    private int numTotalExecutors() {
        int numTotalExecutors = 0;
        for (SingleThreadExecutorPool executorPool : executorPools.values()) {
            numTotalExecutors += executorPool.getNumExecutors();
        }
        return numTotalExecutors;
    }

    private BufferDispatcher createWritingBufferDispatcher(Configuration configuration) {
        BufferDispatcher ret =
                createBufferDispatcher(
                        configuration,
                        MemoryOptions.MEMORY_SIZE_FOR_DATA_WRITING,
                        "WRITING BUFFER POOL");
        StorageMetricsUtil.registerNumAvailableWritingBuffers(ret::numAvailableBuffers);
        StorageMetricsUtil.registerTotalNumWritingBuffers(ret::numTotalBuffers);
        StorageMetricsUtil.registerTimeWaitingWritingBuffers(ret::getLastBufferWaitingTime);
        return ret;
    }

    private BufferDispatcher createReadingBufferDispatcher(Configuration configuration) {
        BufferDispatcher ret =
                createBufferDispatcher(
                        configuration,
                        MemoryOptions.MEMORY_SIZE_FOR_DATA_READING,
                        "READING BUFFER POOL");
        StorageMetricsUtil.registerNumAvailableReadingBuffers(ret::numAvailableBuffers);
        StorageMetricsUtil.registerTotalNumReadingBuffers(ret::numTotalBuffers);
        StorageMetricsUtil.registerTimeWaitingReadingBuffers(ret::getLastBufferWaitingTime);
        return ret;
    }

    private BufferDispatcher createBufferDispatcher(
            Configuration configuration,
            ConfigOption<MemorySize> memorySizeOption,
            String bufferManagerName) {
        CommonUtils.checkArgument(configuration != null, "Must be not null.");
        CommonUtils.checkArgument(memorySizeOption != null, "Must be not null.");

        MemorySize bufferSize =
                checkNotNull(configuration.getMemorySize(MemoryOptions.MEMORY_BUFFER_SIZE));
        if (bufferSize.getBytes() <= 0) {
            throw new ConfigurationException(
                    String.format(
                            "Illegal buffer size configured by %s, must be positive.",
                            MemoryOptions.MEMORY_BUFFER_SIZE.key()));
        }

        MemorySize memorySize = checkNotNull(configuration.getMemorySize(memorySizeOption));
        if (memorySize.getBytes() < MemoryOptions.MIN_VALID_MEMORY_SIZE.getBytes()) {
            throw new ConfigurationException(
                    String.format(
                            "The configured value of %s must be larger than %s.",
                            memorySizeOption.key(),
                            MemoryOptions.MIN_VALID_MEMORY_SIZE.toHumanReadableString()));
        }

        int numBuffers = CommonUtils.checkedDownCast(memorySize.getBytes() / bufferSize.getBytes());
        if (numBuffers <= 0) {
            throw new ConfigurationException(
                    String.format(
                            "The configured value of %s must be no smaller than the configured "
                                    + "value of %s.",
                            memorySizeOption.key(), MemoryOptions.MEMORY_BUFFER_SIZE.key()));
        }

        return new BufferDispatcher(
                bufferManagerName, numBuffers, CommonUtils.checkedDownCast(bufferSize.getBytes()));
    }

    private SimpleSingleThreadExecutorPool createExecutorPool(StorageMeta storageMeta) {
        CommonUtils.checkArgument(storageMeta != null, "Must be not null.");

        ConfigOption<Integer> configOption;
        switch (storageMeta.getStorageType()) {
            case SSD:
                configOption = StorageOptions.STORAGE_SSD_NUM_EXECUTOR_THREADS;
                break;
            case HDD:
                configOption = StorageOptions.STORAGE_NUM_THREADS_PER_HDD;
                break;
            case MEMORY:
                configOption = StorageOptions.STORAGE_MEMORY_NUM_EXECUTOR_THREADS;
                break;
            default:
                throw new ShuffleException("Illegal storage type.");
        }

        Integer numThreads = configuration.getInteger(configOption);
        if (numThreads == null || numThreads <= 0) {
            // if negative value is configured, configuration exception will be thrown
            throw new ConfigurationException(
                    String.format(
                            "The configured value of %s must be positive.", configOption.key()));
        }

        // the actual number of threads will be min[configured value, 4 * (number of processors)]
        numThreads = Math.min(numThreads, 4 * Runtime.getRuntime().availableProcessors());
        return new SimpleSingleThreadExecutorPool(numThreads, "datastore-executor-thread");
    }

    @Override
    public DataPartitionWritingView createDataPartitionWritingView(WritingViewContext context)
            throws Exception {
        DataPartition dataPartition = null;
        boolean isNewDataPartition = false;
        DataPartitionFactory factory =
                partitionFactories.get(context.getPartitionFactoryClassName());
        if (factory == null) {
            throw new ShuffleException(
                    "Can not find target partition factory in classpath or partition factory "
                            + "initialization failed.");
        }

        synchronized (lock) {
            CommonUtils.checkState(!isShutDown, "Data store has been shut down.");

            DataSet dataSet = dataSets.get(context.getDataSetID());
            if (dataSet != null && dataSet.containsDataPartition(context.getDataPartitionID())) {
                dataPartition = dataSet.getDataPartition(context.getDataPartitionID());
            }

            if (dataPartition == null) {
                isNewDataPartition = true;
                dataPartition =
                        factory.createDataPartition(
                                this,
                                context.getJobID(),
                                context.getDataSetID(),
                                context.getDataPartitionID(),
                                context.getNumReducePartitions());
                try {
                    partitionStateListener.onPartitionCreated(dataPartition.getPartitionMeta());
                    addDataPartition(dataPartition);
                } catch (Throwable throwable) {
                    onPartitionAddFailure(dataPartition, throwable);
                    throw throwable;
                }
            }
        }

        try {
            DataPartitionWriter partitionWriter =
                    dataPartition.createPartitionWriter(
                            context.getMapPartitionID(),
                            context.getDataRegionCreditListener(),
                            context.getFailureListener());
            return new PartitionWritingViewImpl(partitionWriter);
        } catch (Throwable throwable) {
            if (isNewDataPartition) {
                // the new data partition should not hold any resource
                removeDataPartition(dataPartition.getPartitionMeta());
            }
            throw throwable;
        }
    }

    @Override
    public DataPartitionReadingView createDataPartitionReadingView(ReadingViewContext context)
            throws Exception {
        DataPartition dataPartition =
                getDataPartition(context.getDataSetID(), context.getPartitionID());
        if (dataPartition == null) {
            // throw partition not found exception if it could not find the target data partition
            throw new PartitionNotFoundException(
                    context.getDataSetID(),
                    context.getPartitionID(),
                    "can not be found in data store, possibly released");
        }

        if (!dataPartition.isConsumable()) {
            PartitionNotFoundException exception =
                    new PartitionNotFoundException(
                            context.getDataSetID(),
                            context.getPartitionID(),
                            "released or not consumable");
            CommonUtils.runQuietly(() -> releaseDataPartition(dataPartition, exception, true));
            throw exception;
        }

        DataPartitionReader partitionReader =
                dataPartition.createPartitionReader(
                        context.getStartPartitionIndex(),
                        context.getEndPartitionIndex(),
                        context.getDataListener(),
                        context.getBacklogListener(),
                        context.getFailureListener());
        return new PartitionReadingViewImpl(partitionReader);
    }

    @Override
    public boolean isDataPartitionConsumable(DataPartitionMeta partitionMeta) {
        CommonUtils.checkArgument(partitionMeta != null, "Must be not null.");

        synchronized (lock) {
            DataSet dataSet = dataSets.get(partitionMeta.getDataSetID());
            if (dataSet == null) {
                return false;
            }

            DataPartition dataPartition =
                    dataSet.getDataPartition(partitionMeta.getDataPartitionID());
            if (dataPartition == null) {
                return false;
            }

            return dataPartition.isConsumable();
        }
    }

    /**
     * Failure handler in case of adding {@link DataPartition}s to this data store fails. It should
     * never happen by design, we add this to catch potential bugs.
     */
    private void onPartitionAddFailure(DataPartition dataPartition, Throwable throwable) {
        CommonUtils.checkArgument(dataPartition != null, "Must be not null");

        boolean removePartition = !(throwable instanceof DuplicatedPartitionException);
        CommonUtils.runQuietly(
                () -> releaseDataPartition(dataPartition, throwable, removePartition));

        DataPartitionMeta partitionMeta = dataPartition.getPartitionMeta();
        LOG.error("Fatal: failed to add data partition: {}.", partitionMeta, throwable);
    }

    @Override
    public void addDataPartition(DataPartitionMeta partitionMeta) throws Exception {
        DataPartitionFactory factory =
                checkNotNull(partitionFactories.get(partitionMeta.getPartitionFactoryClassName()));

        final DataPartition dataPartition;
        try {
            // DataPartitionFactory#createDataPartition method must release
            // all data partition resources itself if any exception occurs
            dataPartition = factory.createDataPartition(this, partitionMeta);
        } catch (Throwable throwable) {
            CommonUtils.runQuietly(() -> partitionStateListener.onPartitionRemoved(partitionMeta));
            LOG.error("Failed to reconstruct data partition from meta.", throwable);
            throw throwable;
        }

        try {
            addDataPartition(dataPartition);
        } catch (Throwable throwable) {
            onPartitionAddFailure(dataPartition, throwable);
            throw throwable;
        }
    }

    @Override
    public void removeDataPartition(DataPartitionMeta partitionMeta) {
        CommonUtils.checkArgument(partitionMeta != null, "Must be not null");

        synchronized (lock) {
            DataSetID dataSetID = partitionMeta.getDataSetID();
            DataSet dataSet = dataSets.get(dataSetID);

            if (dataSet != null) {
                dataSet.removeDataPartition(partitionMeta.getDataPartitionID());
            }

            Set<DataSetID> dataSetIDS = null;
            if (dataSet != null && dataSet.getNumDataPartitions() == 0) {
                dataSetIDS = dataSetsByJob.get(dataSet.getJobID());
                dataSetIDS.remove(dataSetID);
                dataSets.remove(dataSetID);
            }

            if (dataSetIDS != null && dataSetIDS.isEmpty()) {
                dataSetsByJob.remove(dataSet.getJobID());
            }
        }
        CommonUtils.runQuietly(() -> partitionStateListener.onPartitionRemoved(partitionMeta));
    }

    /**
     * Releases the target {@link DataPartition} and notifies the corresponding {@link
     * PartitionStateListener} if the {@link DataPartition} is released successfully.
     */
    private void releaseDataPartition(
            DataPartition dataPartition, Throwable releaseCause, boolean removePartition) {
        if (dataPartition == null) {
            return;
        }

        DataPartitionMeta partitionMeta = dataPartition.getPartitionMeta();
        CompletableFuture<?> future =
                DataPartitionUtils.releaseDataPartition(dataPartition, releaseCause);
        future.whenComplete(
                (ignored, throwable) -> {
                    if (throwable != null) {
                        LOG.error(
                                "Failed to release data partition: {}.", partitionMeta, throwable);
                        return;
                    }

                    if (removePartition) {
                        removeDataPartition(dataPartition.getPartitionMeta());
                    } else {
                        CommonUtils.runQuietly(
                                () -> partitionStateListener.onPartitionRemoved(partitionMeta));
                    }
                    LOG.info("Successfully released data partition: {}.", partitionMeta);
                });
    }

    private void addDataPartition(DataPartition dataPartition) {
        CommonUtils.checkArgument(dataPartition != null, "Must be not null.");

        DataPartitionMeta partitionMeta = dataPartition.getPartitionMeta();
        synchronized (lock) {
            CommonUtils.checkState(!isShutDown, "Data store has been shut down.");

            DataSet dataSet =
                    dataSets.computeIfAbsent(
                            partitionMeta.getDataSetID(),
                            (dataSetID) -> new DataSet(partitionMeta.getJobID(), dataSetID));
            dataSet.addDataPartition(dataPartition);

            Set<DataSetID> dataSetIDS =
                    dataSetsByJob.computeIfAbsent(
                            partitionMeta.getJobID(), (ignored) -> new HashSet<>());
            dataSetIDS.add(partitionMeta.getDataSetID());
        }
    }

    private DataPartition getDataPartition(DataSetID dataSetID, DataPartitionID partitionID) {
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");
        CommonUtils.checkArgument(partitionID != null, "Must be not null.");

        synchronized (lock) {
            CommonUtils.checkState(!isShutDown, "Data store has been shut down.");

            DataPartition dataPartition = null;
            DataSet dataSet = dataSets.get(dataSetID);
            if (dataSet != null) {
                dataPartition = dataSet.getDataPartition(partitionID);
            }

            return dataPartition;
        }
    }

    @Override
    public void releaseDataPartition(
            DataSetID dataSetID, DataPartitionID partitionID, @Nullable Throwable throwable) {
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");
        CommonUtils.checkArgument(partitionID != null, "Must be not null.");

        DataPartition dataPartition = getDataPartition(dataSetID, partitionID);
        CommonUtils.runQuietly(() -> releaseDataPartition(dataPartition, throwable, true));
    }

    @Override
    public void releaseDataSet(DataSetID dataSetID, @Nullable Throwable throwable) {
        CommonUtils.checkArgument(dataSetID != null, "Must be not null.");

        List<DataPartition> dataPartitions = new ArrayList<>();
        synchronized (lock) {
            DataSet dataSet = dataSets.get(dataSetID);
            if (dataSet != null) {
                dataPartitions.addAll(dataSet.getDataPartitions());
            }
        }

        for (DataPartition dataPartition : dataPartitions) {
            CommonUtils.runQuietly(() -> releaseDataPartition(dataPartition, throwable, true));
        }
    }

    @Override
    public void releaseDataByJobID(JobID jobID, @Nullable Throwable throwable) {
        CommonUtils.checkArgument(jobID != null, "Must be not null.");

        List<DataPartition> dataPartitions = new ArrayList<>();
        synchronized (lock) {
            Set<DataSetID> dataSetIDS = dataSetsByJob.get(jobID);

            if (dataSetIDS != null) {
                for (DataSetID dataSetID : dataSetIDS) {
                    dataPartitions.addAll(dataSets.get(dataSetID).getDataPartitions());
                }
            }
        }

        for (DataPartition dataPartition : dataPartitions) {
            CommonUtils.runQuietly(() -> releaseDataPartition(dataPartition, throwable, true));
        }
    }

    @Override
    public void updateUsedStorageSpace() {
        Map<String, Long> storageUsedBytes = new HashMap<>();
        synchronized (lock) {
            for (DataSet dataSet : dataSets.values()) {
                for (DataPartition dataPartition : dataSet.getDataPartitions()) {
                    DataPartitionStatistics statistics = dataPartition.getDataPartitionStatistics();
                    long numBytes = statistics.getDataFileBytes() + statistics.getIndexFileBytes();
                    storageUsedBytes.compute(
                            dataPartition.getPartitionMeta().getStorageMeta().getStorageName(),
                            (k, v) -> v == null ? numBytes : v + numBytes);
                }
            }
        }
        for (DataPartitionFactory factory : partitionFactories.values()) {
            factory.updateUsedStorageSpace(storageUsedBytes);
        }
    }

    @Override
    public DataStoreStatistics getDataStoreStatistics() {
        int numDataPartitions = 0;
        long maxNumDataRegions = 0;
        long maxIndexFileBytes = 0;
        long maxDataFileBytes = 0;
        long totalIndexFileBytes = 0;
        long totalDataFileBytes = 0;
        synchronized (lock) {
            for (DataSet dataSet : dataSets.values()) {
                for (DataPartition dataPartition : dataSet.getDataPartitions()) {
                    ++numDataPartitions;
                    DataPartitionStatistics statistics = dataPartition.getDataPartitionStatistics();
                    maxNumDataRegions = Math.max(maxNumDataRegions, statistics.getNumDataRegions());
                    totalIndexFileBytes += statistics.getIndexFileBytes();
                    totalDataFileBytes += statistics.getDataFileBytes();
                    maxIndexFileBytes = Math.max(maxIndexFileBytes, statistics.getIndexFileBytes());
                    maxDataFileBytes = Math.max(maxDataFileBytes, statistics.getDataFileBytes());
                }
            }
        }
        return new DataStoreStatistics(
                numDataPartitions,
                maxNumDataRegions,
                totalIndexFileBytes,
                totalDataFileBytes,
                maxIndexFileBytes,
                maxDataFileBytes);
    }

    @Override
    public void shutDown(boolean releaseData) {
        LOG.info(String.format("Shutting down the data store: releaseData=%s.", releaseData));

        List<DataPartition> dataPartitions = new ArrayList<>();
        synchronized (lock) {
            if (isShutDown) {
                return;
            }
            isShutDown = true;

            if (releaseData) {
                List<DataSet> dataSetList = new ArrayList<>(dataSets.values());
                dataSets.clear();
                dataSetsByJob.clear();

                for (DataSet dataSet : dataSetList) {
                    dataPartitions.addAll(dataSet.clearDataPartitions());
                }
            }
        }

        DataPartitionUtils.releaseDataPartitions(
                dataPartitions, new ShuffleException("Shutting down."), partitionStateListener);

        destroyBufferDispatcher(writingBufferDispatcher);
        destroyBufferDispatcher(readingBufferDispatcher);

        destroyExecutorPools();
    }

    @Override
    public boolean isShutDown() {
        synchronized (lock) {
            return isShutDown;
        }
    }

    /**
     * Destroys the target {@link BufferDispatcher} and logs the error if encountering any
     * exception.
     */
    private void destroyBufferDispatcher(BufferDispatcher bufferDispatcher) {
        try {
            CommonUtils.checkArgument(bufferDispatcher != null, "Must be not null.");

            bufferDispatcher.destroy();
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to destroy buffer manager.", throwable);
        }
    }

    /**
     * Destroys all the {@link SingleThreadExecutorPool}s and logs the error if encountering any
     * exception.
     */
    private void destroyExecutorPools() {
        synchronized (lock) {
            for (SingleThreadExecutorPool executorPool : executorPools.values()) {
                try {
                    executorPool.destroy();
                } catch (Throwable throwable) {
                    LOG.error("Fatal: failed to destroy executor pool.", throwable);
                }
            }
            executorPools.clear();
        }
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public BufferDispatcher getWritingBufferDispatcher() {
        return writingBufferDispatcher;
    }

    @Override
    public BufferDispatcher getReadingBufferDispatcher() {
        return readingBufferDispatcher;
    }

    @Override
    public SingleThreadExecutorPool getExecutorPool(StorageMeta storageMeta) {
        synchronized (lock) {
            if (isShutDown) {
                throw new ShuffleException("Data store has been already shutdown.");
            }

            return executorPools.computeIfAbsent(storageMeta, this::createExecutorPool);
        }
    }

    @Override
    public void updateFreeStorageSpace() {
        for (DataPartitionFactory partitionFactory : partitionFactories.values()) {
            partitionFactory.updateFreeStorageSpace();
        }
    }

    @Override
    public Map<String, StorageSpaceInfo> getStorageSpaceInfos() {
        Map<String, StorageSpaceInfo> storageSpaceInfos = new HashMap<>();
        for (Map.Entry<String, DataPartitionFactory> entry : partitionFactories.entrySet()) {
            storageSpaceInfos.put(entry.getKey(), entry.getValue().getStorageSpaceInfo());
        }
        return storageSpaceInfos;
    }

    @Override
    public void updateStorageHealthStatus() {
        for (DataPartitionFactory partitionFactory : partitionFactories.values()) {
            partitionFactory.updateStorageHealthStatus();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // For test
    // ---------------------------------------------------------------------------------------------

    Map<JobID, Map<DataSetID, Set<DataPartitionID>>> getStoredData() {
        Map<JobID, Map<DataSetID, Set<DataPartitionID>>> dataSetsByJobMap = new HashMap<>();
        synchronized (lock) {
            for (Map.Entry<JobID, Set<DataSetID>> entry : dataSetsByJob.entrySet()) {
                JobID jobID = entry.getKey();
                Map<DataSetID, Set<DataPartitionID>> dataSetPartitions = new HashMap<>();
                dataSetsByJobMap.put(jobID, dataSetPartitions);

                for (Map.Entry<DataSetID, DataSet> dataSetEntry : dataSets.entrySet()) {
                    if (dataSetEntry.getValue().getJobID().equals(jobID)) {
                        dataSetPartitions.put(
                                dataSetEntry.getKey(),
                                dataSetEntry.getValue().getDataPartitionIDs());
                    }
                }
            }
        }
        return dataSetsByJobMap;
    }
}
