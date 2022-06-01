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

package com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.coordinator.metrics.ClusterMetricsUtil;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * Tracks the status of the current jobs, workers and the data partitions.
 *
 * <p>A data partition might be in three status:
 *
 * <ol>
 *   <li>Normal
 *   <li>Releasing
 *   <li>Released
 * </ol>
 *
 * <p>For each job, we only maintain normal data partitions and their assigned workers. For each
 * worker, we maintain both the normal and releasing data partitions, and the releasing data
 * partitions would be removed once the workers have indeed released the data partitions.
 */
public class AssignmentTrackerImpl implements AssignmentTracker {

    private static final Logger LOG = LoggerFactory.getLogger(AssignmentTrackerImpl.class);

    /** {@link PartitionPlacementStrategy} used to select the proper shuffle worker. */
    private final PartitionPlacementStrategy partitionPlacementStrategy;

    /** All available {@link DataPartitionFactory}s in the classpath. */
    private final Map<String, DataPartitionFactory> partitionFactories = new HashMap<>();

    /** The currently registered jobs. */
    private final Map<JobID, JobStatus> jobs = new HashMap<>();

    /** The currently registered workers. */
    private final Map<RegistrationID, WorkerStatus> workers = new ConcurrentHashMap<>();

    public AssignmentTrackerImpl(Configuration configuration) {
        this.partitionPlacementStrategy =
                PartitionPlacementStrategyLoader.loadPlacementStrategyFactory(configuration);
        registerMetrics();

        ServiceLoader<DataPartitionFactory> serviceLoader =
                ServiceLoader.load(DataPartitionFactory.class);
        for (DataPartitionFactory partitionFactory : serviceLoader) {
            partitionFactories.put(partitionFactory.getClass().getName(), partitionFactory);
        }
        CommonUtils.checkState(!partitionFactories.isEmpty(), "No valid partition factory found.");
    }

    private long countDataPartitions() {
        long numDataPartitions = 0;
        for (WorkerStatus worker : workers.values()) {
            numDataPartitions += worker.numDataPartitions();
        }
        return numDataPartitions;
    }

    private void registerMetrics() {
        ClusterMetricsUtil.registerTotalNumDataPartitions(this::countDataPartitions);
        ClusterMetricsUtil.registerNumJobsServing(jobs::size);
        ClusterMetricsUtil.registerNumShuffleWorkers(workers::size);
        ClusterMetricsUtil.registerHddMaxFreeBytes(this::getHddMaxStorageFreeSpace);
        ClusterMetricsUtil.registerSsdMaxFreeBytes(this::getSsdMaxStorageFreeSpace);
        ClusterMetricsUtil.registerHddMaxUsedBytes(this::getHddMaxStorageUsedSpace);
        ClusterMetricsUtil.registerSsdMaxUsedBytes(this::getSsdMaxStorageUsedSpace);
    }

    public long getHddMaxStorageFreeSpace() {
        long hddMaxUsableSpace = 0;
        for (WorkerStatus worker : workers.values()) {
            hddMaxUsableSpace = Math.max(hddMaxUsableSpace, worker.getHddMaxStorageFreeSpace());
        }
        return hddMaxUsableSpace;
    }

    public long getSsdMaxStorageFreeSpace() {
        long ssdMaxUsableSpace = 0;
        for (WorkerStatus worker : workers.values()) {
            ssdMaxUsableSpace = Math.max(ssdMaxUsableSpace, worker.getSsdMaxStorageFreeSpace());
        }
        return ssdMaxUsableSpace;
    }

    public long getHddMaxStorageUsedSpace() {
        long hddMaxUsableSpace = 0;
        for (WorkerStatus worker : workers.values()) {
            hddMaxUsableSpace = Math.max(hddMaxUsableSpace, worker.getHddMaxStorageUsedSpace());
        }
        return hddMaxUsableSpace;
    }

    public long getSsdMaxStorageUsedSpace() {
        long ssdMaxUsableSpace = 0;
        for (WorkerStatus worker : workers.values()) {
            ssdMaxUsableSpace = Math.max(ssdMaxUsableSpace, worker.getSsdMaxStorageUsedSpace());
        }
        return ssdMaxUsableSpace;
    }

    @Override
    public boolean isWorkerRegistered(RegistrationID registrationID) {
        return workers.containsKey(registrationID);
    }

    @Override
    public void registerWorker(
            InstanceID workerID,
            RegistrationID registrationID,
            ShuffleWorkerGateway gateway,
            String externalAddress,
            int dataPort) {

        checkState(
                !workers.containsKey(registrationID),
                String.format("The worker %s has been registered", registrationID));

        WorkerStatus worker =
                new WorkerStatus(workerID, registrationID, gateway, externalAddress, dataPort);
        workers.put(registrationID, worker);
        partitionPlacementStrategy.addWorker(worker);
    }

    @Override
    public void workerReportDataPartitionReleased(
            RegistrationID workerRegistrationID,
            JobID jobID,
            DataSetID dataSetID,
            DataPartitionID dataPartitionID) {

        WorkerStatus workerStatus = workers.get(workerRegistrationID);
        internalNotifyWorkerToRemoveReleasedDataPartition(
                jobID, new DataPartitionCoordinate(dataSetID, dataPartitionID), workerStatus);
    }

    @Override
    public void synchronizeWorkerDataPartitions(
            RegistrationID workerRegistrationID, List<DataPartitionStatus> reportedDataPartitions) {

        WorkerStatus workerStatus = workers.get(workerRegistrationID);
        if (workerStatus == null) {
            LOG.warn("Received report from unknown worker {}", workerRegistrationID);
            return;
        }

        // First ensure the JM have recorded all the possible data partitions.
        for (DataPartitionStatus reportedDataPartition : reportedDataPartitions) {
            if (!workerStatus
                    .getDataPartitions()
                    .containsKey(reportedDataPartition.getCoordinate())) {
                internalAddDataPartition(workerStatus, reportedDataPartition);
            }
        }

        Map<DataPartitionCoordinate, DataPartitionStatus> reportedStatusMap = new HashMap<>();
        reportedDataPartitions.forEach(
                status -> reportedStatusMap.put(status.getCoordinate(), status));

        List<DataPartitionStatus> notifyWorkerToReleaseData = new ArrayList<>();
        List<DataPartitionStatus> notifyWorkerToReleaseMeta = new ArrayList<>();
        List<DataPartitionStatus> workerHasReleasedMeta = new ArrayList<>();

        for (Map.Entry<DataPartitionCoordinate, DataPartitionStatus> entry :
                workerStatus.getDataPartitions().entrySet()) {
            DataPartitionStatus status = entry.getValue();

            DataPartitionStatus reportedStatus = reportedStatusMap.get(status.getCoordinate());

            if (status.isReleasing()) {
                if (reportedStatus == null) {
                    // We have removed the meta for this data partition, then
                    // the master could also remove the meta
                    workerHasReleasedMeta.add(status);
                } else if (reportedStatus.isReleasing()) {
                    // Now the manager is aware of the releasing, then it
                    // asks the worker to remove the remaining meta.
                    notifyWorkerToReleaseMeta.add(status);
                } else if (!reportedStatus.isReleasing()) {
                    // Worker might still not know we are going to remove the data
                    // partition.
                    notifyWorkerToReleaseData.add(status);
                }
            } else {
                if (reportedStatus != null && reportedStatus.isReleasing()) {
                    // Worker initiate the releasing, the master then synchronize
                    // the status and asks the worker to remove the remaining meta.
                    status.setReleasing(true);
                    notifyWorkerToReleaseMeta.add(status);
                }
            }
        }

        notifyWorkerToReleaseData.forEach(
                status ->
                        internalReleaseDataPartition(
                                status.getJobId(), status.getCoordinate(), workerStatus));

        notifyWorkerToReleaseMeta.forEach(
                status ->
                        internalNotifyWorkerToRemoveReleasedDataPartition(
                                status.getJobId(), status.getCoordinate(), workerStatus));

        workerHasReleasedMeta.forEach(
                status -> workerStatus.removeReleasedDataPartition(status.getCoordinate()));
    }

    @Override
    public void unregisterWorker(RegistrationID workerRegistrationID) {
        WorkerStatus workerStatus = workers.remove(workerRegistrationID);

        if (workerStatus == null) {
            return;
        }
        partitionPlacementStrategy.removeWorker(workerStatus);

        for (DataPartitionStatus status : workerStatus.getDataPartitions().values()) {
            JobStatus jobStatus = jobs.get(status.getJobId());
            if (jobStatus != null
                    && jobStatus.getDataPartitions().containsKey(status.getCoordinate())) {
                if (!Objects.equals(
                        jobStatus
                                .getDataPartitions()
                                .get(status.getCoordinate())
                                .getRegistrationID(),
                        workerStatus.getRegistrationID())) {
                    LOG.warn(
                            "Inconsistency happens: job think the partition {} is on {}, in fact it is on {}",
                            status,
                            jobStatus.getDataPartitions().get(status.getCoordinate()),
                            workerStatus);
                }

                jobStatus.removeDataPartition(status.getCoordinate());
            }
        }
    }

    @Override
    public boolean isJobRegistered(JobID jobID) {
        return jobs.containsKey(jobID);
    }

    @Override
    public void registerJob(JobID jobID) {
        jobs.put(jobID, new JobStatus());
    }

    @Override
    public void unregisterJob(JobID jobID) {
        JobStatus jobStatus = jobs.remove(jobID);

        if (jobStatus == null) {
            return;
        }

        jobStatus
                .getDataPartitions()
                .forEach(
                        (id, workerStatus) -> {
                            DataPartitionStatus dataPartitionStatus =
                                    workerStatus.getDataPartitions().get(id);
                            if (dataPartitionStatus != null) {
                                internalReleaseDataPartition(
                                        dataPartitionStatus.getJobId(),
                                        dataPartitionStatus.getCoordinate(),
                                        workerStatus);
                            }
                        });
    }

    @Override
    public ShuffleResource requestShuffleResource(
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapPartitionID,
            int numberOfConsumers,
            String dataPartitionFactoryName,
            String taskLocation)
            throws ShuffleResourceAllocationException {
        JobStatus jobStatus = jobs.get(jobID);

        if (jobStatus == null) {
            throw new ShuffleResourceAllocationException(
                    "Job is not registered before requesting resources.");
        }

        DataPartitionFactory partitionFactory = partitionFactories.get(dataPartitionFactoryName);
        if (partitionFactory == null) {
            throw new ShuffleResourceAllocationException(
                    "Unknown data partition factory: " + dataPartitionFactoryName);
        }
        WorkerStatus oldStatus =
                jobStatus
                        .getDataPartitions()
                        .get(new DataPartitionCoordinate(dataSetID, mapPartitionID));
        if (oldStatus != null) {
            ShuffleWorkerDescriptor descriptor = oldStatus.createShuffleWorkerDescriptor();
            LOG.warn(
                    "The request data partition {}-{}-{} has been allocated on {}",
                    jobID,
                    dataSetID,
                    mapPartitionID,
                    descriptor);
            return new DefaultShuffleResource(
                    new ShuffleWorkerDescriptor[] {descriptor},
                    partitionFactory.getDataPartitionType());
        }

        WorkerStatus[] selectedWorkerStatuses =
                partitionPlacementStrategy.selectNextWorker(
                        new PartitionPlacementContext(partitionFactory, taskLocation));
        checkState(
                selectedWorkerStatuses.length == 1,
                "Currently only one worker need to be selected");
        WorkerStatus workerStatus = selectedWorkerStatuses[0];

        internalAddDataPartition(
                workerStatus,
                new DataPartitionStatus(
                        jobID, new DataPartitionCoordinate(dataSetID, mapPartitionID)));

        return new DefaultShuffleResource(
                new ShuffleWorkerDescriptor[] {workerStatus.createShuffleWorkerDescriptor()},
                partitionFactory.getDataPartitionType());
    }

    @Override
    public void releaseShuffleResource(
            JobID jobID, DataSetID dataSetID, MapPartitionID mapPartitionID) {
        // Try to find the worker serving this data
        JobStatus jobStatus = jobs.get(jobID);
        if (jobStatus != null) {
            DataPartitionCoordinate coordinate =
                    new DataPartitionCoordinate(dataSetID, mapPartitionID);
            WorkerStatus workerStatus = jobStatus.getDataPartitions().get(coordinate);
            internalReleaseDataPartition(jobID, coordinate, workerStatus);
        }
    }

    @Override
    public ChangedWorkerStatus computeChangedWorkers(
            JobID jobID,
            Collection<InstanceID> cachedWorkerList,
            boolean considerUnrelatedWorkers) {
        JobStatus jobStatus = jobs.get(jobID);

        Map<InstanceID, RegistrationID> remainedWorkers = new HashMap<>();

        List<InstanceID> unrelatedWorkers = new ArrayList<>();
        Map<InstanceID, Set<DataPartitionCoordinate>> newlyRelatedWorkers = new HashMap<>();

        jobStatus
                .getDataPartitions()
                .forEach(
                        (dataPartition, worker) -> {
                            if (worker.getDataPartitions().get(dataPartition).isReleasing()) {
                                return;
                            }

                            remainedWorkers.put(worker.getWorkerID(), worker.getRegistrationID());
                            if (!cachedWorkerList.contains(worker.getWorkerID())) {
                                newlyRelatedWorkers
                                        .computeIfAbsent(worker.getWorkerID(), k -> new HashSet<>())
                                        .add(dataPartition);
                            }
                        });

        if (considerUnrelatedWorkers) {
            for (InstanceID workerID : cachedWorkerList) {
                if (!remainedWorkers.containsKey(workerID)) {
                    unrelatedWorkers.add(workerID);
                } else {
                    // The following is a safeguard for unexpected situations.
                    RegistrationID registrationID = remainedWorkers.get(workerID);
                    if (!workers.containsKey(registrationID)) {
                        LOG.warn(
                                "Inconsistency: Remaining partitions on removed worker {}",
                                workerID);
                        unrelatedWorkers.add(workerID);
                    }
                }
            }
        }

        return new ChangedWorkerStatus(unrelatedWorkers, newlyRelatedWorkers);
    }

    @Override
    public List<JobID> listJobs() {
        return new ArrayList<>(jobs.keySet());
    }

    @Override
    public int getNumberOfWorkers() {
        return workers.size();
    }

    @Override
    public Map<DataPartitionCoordinate, InstanceID> getDataPartitionDistribution(JobID jobID) {
        return jobs.get(jobID).getDataPartitions().entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getWorkerID()));
    }

    @Override
    public void reportWorkerStorageSpaces(
            InstanceID instanceID,
            RegistrationID workerRegistrationID,
            Map<String, StorageSpaceInfo> storageSpaceInfos) {
        WorkerStatus workerStatus = workers.get(workerRegistrationID);
        if (workerStatus == null) {
            LOG.warn("Received worker storage spaces from unknown worker {}", workerRegistrationID);
            return;
        }

        checkState(instanceID.equals(workerStatus.getWorkerID()));
        workerStatus.updateStorageSpaceInfo(storageSpaceInfos);
    }

    public Map<JobID, JobStatus> getJobs() {
        return jobs;
    }

    public Map<RegistrationID, WorkerStatus> getWorkers() {
        return workers;
    }

    private void internalReleaseDataPartition(
            JobID jobId, DataPartitionCoordinate coordinate, @Nullable WorkerStatus workerStatus) {

        JobStatus jobStatus = jobs.get(jobId);
        if (jobStatus != null) {
            jobStatus.removeDataPartition(coordinate);
        }

        if (workerStatus != null) {
            workerStatus.markAsReleasing(jobId, coordinate);
            workerStatus
                    .getGateway()
                    .releaseDataPartition(
                            jobId, coordinate.getDataSetId(), coordinate.getDataPartitionId());
        }
    }

    private void internalNotifyWorkerToRemoveReleasedDataPartition(
            JobID jobId, DataPartitionCoordinate coordinate, @Nullable WorkerStatus workerStatus) {

        JobStatus jobStatus = jobs.get(jobId);
        if (jobStatus != null) {
            jobStatus.removeDataPartition(coordinate);
        }

        if (workerStatus != null) {
            workerStatus.markAsReleasing(jobId, coordinate);
            workerStatus
                    .getGateway()
                    .removeReleasedDataPartitionMeta(
                            jobId, coordinate.getDataSetId(), coordinate.getDataPartitionId());
        }
    }

    private void internalAddDataPartition(
            WorkerStatus workerStatus, DataPartitionStatus dataPartitionStatus) {

        checkState(
                jobs.containsKey(dataPartitionStatus.getJobId()),
                "A data partition is added before job registered.");

        // If this data partition is maintained by another worker,
        // we remove the record to keep the data consistent.
        // This might happen when the restarted worker registered
        // before the original one timeout.
        JobStatus jobStatus = jobs.get(dataPartitionStatus.getJobId());
        WorkerStatus oldWorkerStatus =
                jobStatus.getDataPartitions().get(dataPartitionStatus.getCoordinate());

        if (oldWorkerStatus != null) {
            oldWorkerStatus.removeReleasedDataPartition(dataPartitionStatus.getCoordinate());
        }

        jobs.get(dataPartitionStatus.getJobId())
                .addDataPartition(dataPartitionStatus.getCoordinate(), workerStatus);
        workerStatus.addDataPartition(dataPartitionStatus);
    }
}
