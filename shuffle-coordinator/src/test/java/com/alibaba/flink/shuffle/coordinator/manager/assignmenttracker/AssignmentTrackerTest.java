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

package com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionStatus;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.coordinator.utils.EmptyShuffleWorkerGateway;
import com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.UsableStorageSpaceInfo;
import com.alibaba.flink.shuffle.rpc.message.Acknowledge;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomDataSetId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomJobId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomMapPartitionId;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests the assignment tracker implementation by {@link AssignmentTrackerImpl}. */
public class AssignmentTrackerTest {
    private static final Logger LOG = LoggerFactory.getLogger(AssignmentTrackerTest.class);

    private static final String partitionFactory =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory";

    @Test
    public void testWorkerRegistration() {
        RegistrationID registrationID = new RegistrationID();

        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(new Configuration());
        assertFalse(assignmentTracker.isWorkerRegistered(registrationID));

        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("test"),
                registrationID,
                new EmptyShuffleWorkerGateway(),
                "",
                1024);
        assertTrue(assignmentTracker.isWorkerRegistered(registrationID));
    }

    @Test
    public void testJobRegistration() {
        JobID jobId = randomJobId();

        AssignmentTracker assignmentTracker = new AssignmentTrackerImpl(new Configuration());
        assertFalse(assignmentTracker.isJobRegistered(jobId));

        assignmentTracker.registerJob(jobId);
        assertTrue(assignmentTracker.isJobRegistered(jobId));
    }

    @Test(expected = ShuffleResourceAllocationException.class)
    public void testRequestResourceWithoutWorker() throws Exception {
        JobID jobId = randomJobId();

        AssignmentTracker assignmentTracker = new AssignmentTrackerImpl(new Configuration());
        assignmentTracker.registerJob(jobId);

        assignmentTracker.requestShuffleResource(
                jobId, randomDataSetId(), randomMapPartitionId(), 3, partitionFactory, null);
    }

    @Test
    public void testRequestResourceWithWorkers() throws Exception {
        JobID jobId = randomJobId();

        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        configuration.setString(
                ManagerOptions.PARTITION_PLACEMENT_STRATEGY,
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME);
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        // Registers two workers
        RegistrationID worker1 = new RegistrationID();
        RegistrationID worker2 = new RegistrationID();

        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker1"),
                worker1,
                new EmptyShuffleWorkerGateway(),
                "worker1",
                1024);
        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker2"),
                worker2,
                new EmptyShuffleWorkerGateway(),
                "worker2",
                1026);

        MapPartitionID dataPartitionId1 = randomMapPartitionId();
        MapPartitionID dataPartitionId2 = randomMapPartitionId();

        List<ShuffleResource> allocatedResources = new ArrayList<>();
        allocatedResources.add(
                assignmentTracker.requestShuffleResource(
                        jobId, randomDataSetId(), dataPartitionId1, 2, partitionFactory, null));
        allocatedResources.add(
                assignmentTracker.requestShuffleResource(
                        jobId, randomDataSetId(), dataPartitionId2, 2, partitionFactory, null));
        assertThat(
                allocatedResources.stream()
                        .map(
                                resource ->
                                        ((DefaultShuffleResource) resource)
                                                .getMapPartitionLocation())
                        .collect(Collectors.toList()),
                containsInAnyOrder(
                        new ShuffleWorkerDescriptor(new InstanceID("worker1"), "worker1", 1024),
                        new ShuffleWorkerDescriptor(new InstanceID("worker2"), "worker2", 1026)));
    }

    @Test
    public void testReAllocation() throws Exception {
        JobID jobId = randomJobId();
        RegistrationID worker1 = new RegistrationID();

        AssignmentTracker assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), new EmptyShuffleWorkerGateway());

        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionId = randomMapPartitionId();
        ShuffleResource shuffleResource1 =
                assignmentTracker.requestShuffleResource(
                        jobId, dataSetId, dataPartitionId, 2, partitionFactory, null);

        // reallocation the same data partition on the same worker should remain unchanged
        ShuffleResource shuffleResource2 =
                assignmentTracker.requestShuffleResource(
                        jobId, dataSetId, dataPartitionId, 2, partitionFactory, null);
        assertEquals(shuffleResource1, shuffleResource2);
    }

    @Test
    public void testSynchronizeStatusFromWorkerWithMissedDataPartitions() throws Exception {
        JobID jobId = randomJobId();
        DataSetID dataSetID = randomDataSetId();
        MapPartitionID dataPartitionID = randomMapPartitionId();

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();

        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.synchronizeWorkerDataPartitions(
                worker1,
                Collections.singletonList(
                        new DataPartitionStatus(
                                jobId,
                                new DataPartitionCoordinate(dataSetID, dataPartitionID),
                                false)));

        assertThat(
                assignmentTracker.getJobs().get(jobId).getDataPartitions().keySet().stream()
                        .map(DataPartitionCoordinate::getDataPartitionId)
                        .collect(Collectors.toList()),
                containsInAnyOrder(dataPartitionID));
    }

    @Test
    public void testSynchronizedStatusWorkerReleasingAndManagerNot() throws Exception {
        JobID jobId = randomJobId();
        DataSetID dataSetID = randomDataSetId();
        MapPartitionID dataPartitionID = randomMapPartitionId();
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(dataSetID, dataPartitionID);

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();

        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetID, dataPartitionID, 2, partitionFactory, null);
        assignmentTracker.synchronizeWorkerDataPartitions(
                worker1,
                Collections.singletonList(
                        new DataPartitionStatus(
                                jobId,
                                new DataPartitionCoordinate(dataSetID, dataPartitionID),
                                true)));

        assertEquals(0, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        WorkerStatus workerStatus = assignmentTracker.getWorkers().get(worker1);
        assertTrue(workerStatus.getDataPartitions().get(coordinate).isReleasing());
        assertEquals(1, shuffleWorkerGateway.getReleaseMetaPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetID, dataPartitionID),
                shuffleWorkerGateway.getReleaseMetaPartitions().get(0));
    }

    @Test
    public void testSynchronizedStatusManagerReleasingAndWorkerNot() throws Exception {
        JobID jobId = randomJobId();
        DataSetID dataSetID = randomDataSetId();
        MapPartitionID dataPartitionID = randomMapPartitionId();
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(dataSetID, dataPartitionID);

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();

        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetID, dataPartitionID, 2, partitionFactory, null);
        assignmentTracker.releaseShuffleResource(jobId, dataSetID, dataPartitionID);
        shuffleWorkerGateway.reset();

        assignmentTracker.synchronizeWorkerDataPartitions(
                worker1,
                Collections.singletonList(
                        new DataPartitionStatus(
                                jobId,
                                new DataPartitionCoordinate(dataSetID, dataPartitionID),
                                false)));

        assertEquals(0, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        WorkerStatus workerStatus = assignmentTracker.getWorkers().get(worker1);
        assertTrue(workerStatus.getDataPartitions().get(coordinate).isReleasing());
        assertEquals(1, shuffleWorkerGateway.getReleasedPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetID, dataPartitionID),
                shuffleWorkerGateway.getReleasedPartitions().get(0));
    }

    @Test
    public void testSynchronizedStatusManagerReleasingAndWorkerReleasing() throws Exception {
        JobID jobId = randomJobId();
        DataSetID dataSetID = randomDataSetId();
        MapPartitionID dataPartitionID = randomMapPartitionId();
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(dataSetID, dataPartitionID);

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();

        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetID, dataPartitionID, 2, partitionFactory, null);
        assignmentTracker.releaseShuffleResource(jobId, dataSetID, dataPartitionID);
        shuffleWorkerGateway.reset();

        assignmentTracker.synchronizeWorkerDataPartitions(
                worker1,
                Collections.singletonList(
                        new DataPartitionStatus(
                                jobId,
                                new DataPartitionCoordinate(dataSetID, dataPartitionID),
                                true)));

        assertEquals(0, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        WorkerStatus workerStatus = assignmentTracker.getWorkers().get(worker1);
        assertTrue(workerStatus.getDataPartitions().get(coordinate).isReleasing());
        assertEquals(1, shuffleWorkerGateway.getReleaseMetaPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetID, dataPartitionID),
                shuffleWorkerGateway.getReleaseMetaPartitions().get(0));
    }

    @Test
    public void testClientReleaseDataPartition() throws Exception {
        JobID jobId = randomJobId();
        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();

        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionId = randomMapPartitionId();
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(dataSetId, dataPartitionId);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionId, 2, partitionFactory, null);

        // Step 1. Client asks to releasing the data partition
        assignmentTracker.releaseShuffleResource(jobId, dataSetId, dataPartitionId);
        assertEquals(1, shuffleWorkerGateway.getReleasedPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetId, dataPartitionId),
                shuffleWorkerGateway.getReleasedPartitions().get(0));

        WorkerStatus workerStatus = assignmentTracker.getWorkers().get(worker1);
        assertTrue(workerStatus.getDataPartitions().get(coordinate).isReleasing());

        // Step 2. If the data partition is not removed on synchronization, would try to remove it
        // again.
        assignmentTracker.synchronizeWorkerDataPartitions(
                worker1,
                Collections.singletonList(
                        new DataPartitionStatus(
                                jobId, new DataPartitionCoordinate(dataSetId, dataPartitionId))));
        assertEquals(2, shuffleWorkerGateway.getReleasedPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetId, dataPartitionId),
                shuffleWorkerGateway.getReleasedPartitions().get(1));

        // Step 3. The Worker remove the data and notifies the manager
        assignmentTracker.workerReportDataPartitionReleased(
                worker1, jobId, dataSetId, dataPartitionId);
        assertEquals(1, shuffleWorkerGateway.getReleaseMetaPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetId, dataPartitionId),
                shuffleWorkerGateway.getReleaseMetaPartitions().get(0));

        // The data would be removed after worker has removed it.
        assignmentTracker.synchronizeWorkerDataPartitions(worker1, Collections.emptyList());
        assertFalse(
                assignmentTracker.getJobs().get(jobId).getDataPartitions().containsKey(coordinate));
        assertFalse(
                assignmentTracker
                        .getWorkers()
                        .get(worker1)
                        .getDataPartitions()
                        .containsKey(coordinate));
    }

    @Test
    public void testWorkerReleaseDataPartition() throws Exception {
        JobID jobId = randomJobId();

        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();
        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker1"),
                worker1,
                shuffleWorkerGateway,
                "worker1",
                1024);

        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionId = randomMapPartitionId();
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(dataSetId, dataPartitionId);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionId, 2, partitionFactory, null);

        // Step 1. Worker reports the data is released
        assignmentTracker.workerReportDataPartitionReleased(
                worker1, jobId, dataSetId, dataPartitionId);
        assertEquals(1, shuffleWorkerGateway.getReleaseMetaPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetId, dataPartitionId),
                shuffleWorkerGateway.getReleaseMetaPartitions().get(0));

        WorkerStatus workerStatus = assignmentTracker.getWorkers().get(worker1);
        assertTrue(workerStatus.getDataPartitions().get(coordinate).isReleasing());

        // Step 2. If the data partition meta is not removed on synchronization, would try to remove
        // it again.
        assignmentTracker.synchronizeWorkerDataPartitions(
                worker1,
                Collections.singletonList(
                        new DataPartitionStatus(
                                jobId,
                                new DataPartitionCoordinate(dataSetId, dataPartitionId),
                                true)));
        assertEquals(2, shuffleWorkerGateway.getReleaseMetaPartitions().size());
        assertEquals(
                Triple.of(jobId, dataSetId, dataPartitionId),
                shuffleWorkerGateway.getReleaseMetaPartitions().get(1));

        // Step 3. The data would be removed after worker has removed it.
        assignmentTracker.synchronizeWorkerDataPartitions(worker1, Collections.emptyList());
        assertFalse(
                assignmentTracker.getJobs().get(jobId).getDataPartitions().containsKey(coordinate));
        assertFalse(
                assignmentTracker
                        .getWorkers()
                        .get(worker1)
                        .getDataPartitions()
                        .containsKey(coordinate));
    }

    @Test
    public void testComputeChangedWorker() throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        RegistrationID worker1 = new RegistrationID();
        RegistrationID worker2 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();

        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        configuration.setString(
                ManagerOptions.PARTITION_PLACEMENT_STRATEGY,
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME);
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker1"),
                worker1,
                shuffleWorkerGateway,
                "worker1",
                1024);
        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker2"),
                worker2,
                new EmptyShuffleWorkerGateway(),
                "worker2",
                1024);

        DataSetID dataSetID = RandomIDUtils.randomDataSetId();
        MapPartitionID mapPartitionID = RandomIDUtils.randomMapPartitionId();

        // Requesting two shuffle resources, which would be assigned in the two workers
        InstanceID firstWorkerId =
                (assignmentTracker.requestShuffleResource(
                                jobId, dataSetID, mapPartitionID, 2, partitionFactory, null))
                        .getMapPartitionLocation()
                        .getWorkerId();
        InstanceID secondWorkerId =
                (assignmentTracker.requestShuffleResource(
                                jobId,
                                RandomIDUtils.randomDataSetId(),
                                RandomIDUtils.randomMapPartitionId(),
                                2,
                                partitionFactory,
                                null))
                        .getMapPartitionLocation()
                        .getWorkerId();
        assertNotEquals(secondWorkerId, firstWorkerId);

        ChangedWorkerStatus changedWorkerStatus =
                assignmentTracker.computeChangedWorkers(
                        jobId,
                        new HashSet<>(Arrays.asList(new InstanceID("dummy"), secondWorkerId)),
                        true);
        assertEquals(
                Collections.singletonList(new InstanceID("dummy")),
                changedWorkerStatus.getIrrelevantWorkers());
        assertEquals(1, changedWorkerStatus.getRelevantWorkers().size());
        Set<DataPartitionCoordinate> dataPartitions =
                changedWorkerStatus.getRelevantWorkers().get(firstWorkerId);
        assertEquals(
                Collections.singleton(new DataPartitionCoordinate(dataSetID, mapPartitionID)),
                dataPartitions);

        ChangedWorkerStatus noUnrelatedWorkerStatus =
                assignmentTracker.computeChangedWorkers(
                        jobId,
                        new HashSet<>(Arrays.asList(new InstanceID("dummy"), secondWorkerId)),
                        false);
        assertEquals(0, noUnrelatedWorkerStatus.getIrrelevantWorkers().size());
        assertEquals(1, noUnrelatedWorkerStatus.getRelevantWorkers().size());
        assertEquals(
                Collections.singleton(new DataPartitionCoordinate(dataSetID, mapPartitionID)),
                dataPartitions);
    }

    @Test
    public void testComputeChangeWorkerSafeGuardTest() throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        RegistrationID worker1 = new RegistrationID("worker1");
        InstanceID worker1InstanceId = new InstanceID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();
        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(jobId, worker1, worker1InstanceId, shuffleWorkerGateway);

        DataSetID dataSetID = RandomIDUtils.randomDataSetId();
        MapPartitionID mapPartitionID = RandomIDUtils.randomMapPartitionId();

        assignmentTracker.requestShuffleResource(
                jobId, dataSetID, mapPartitionID, 2, partitionFactory, null);

        // This is a safe guard, in reality the following should not happen, but
        // we still want to have this.
        assignmentTracker.getWorkers().remove(worker1);

        ChangedWorkerStatus workerStatus =
                assignmentTracker.computeChangedWorkers(
                        jobId, Collections.singleton(worker1InstanceId), true);
        assertEquals(0, workerStatus.getRelevantWorkers().size());
        assertEquals(
                Collections.singletonList(worker1InstanceId), workerStatus.getIrrelevantWorkers());
    }

    @Test
    public void testUnregisterJob() throws Exception {
        JobID jobId = randomJobId();
        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionID1 = randomMapPartitionId();
        MapPartitionID dataPartitionID2 = randomMapPartitionId();

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();
        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionID1, 2, partitionFactory, null);
        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionID2, 2, partitionFactory, null);

        assignmentTracker.unregisterJob(jobId);

        assertEquals(0, assignmentTracker.getJobs().size());

        WorkerStatus workerStatus = assignmentTracker.getWorkers().get(worker1);
        assertEquals(2, workerStatus.getDataPartitions().size());
        for (MapPartitionID partitionId : Arrays.asList(dataPartitionID1, dataPartitionID2)) {
            assertTrue(
                    workerStatus
                            .getDataPartitions()
                            .get(new DataPartitionCoordinate(dataSetId, partitionId))
                            .isReleasing());
        }
    }

    @Test
    public void testUnregisterWorker() throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionID1 = randomMapPartitionId();
        MapPartitionID dataPartitionID2 = randomMapPartitionId();

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();
        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionID1, 2, partitionFactory, null);
        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionID2, 2, partitionFactory, null);

        assignmentTracker.unregisterWorker(worker1);

        assertEquals(0, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        assertEquals(0, assignmentTracker.getWorkers().size());
    }

    @Test
    public void testUnregisterWorkerWithReleasingPartitions()
            throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionID1 = randomMapPartitionId();
        MapPartitionID dataPartitionID2 = randomMapPartitionId();

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway =
                new ReleaseRecordingShuffleWorkerGateway();
        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionID1, 2, partitionFactory, null);
        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionID2, 2, partitionFactory, null);

        assignmentTracker.releaseShuffleResource(jobId, dataSetId, dataPartitionID1);

        assignmentTracker.unregisterWorker(worker1);
        assertEquals(0, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        assertEquals(0, assignmentTracker.getWorkers().size());
    }

    @Test
    public void testShuffleWorkerRestartedBeforeLastTimeout()
            throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        DataSetID dataSetId = randomDataSetId();
        MapPartitionID dataPartitionId = randomMapPartitionId();
        DataPartitionCoordinate coordinate =
                new DataPartitionCoordinate(dataSetId, dataPartitionId);

        RegistrationID worker1 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway1 =
                new ReleaseRecordingShuffleWorkerGateway();
        AssignmentTrackerImpl assignmentTracker =
                createAssignmentTracker(
                        jobId, worker1, new InstanceID("worker1"), shuffleWorkerGateway1);

        assignmentTracker.requestShuffleResource(
                jobId, dataSetId, dataPartitionId, 2, partitionFactory, null);
        assertEquals(1, assignmentTracker.getWorkers().get(worker1).getDataPartitions().size());
        assertEquals(1, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());

        // Now simulates the first worker exit and the second worker registered before the first
        // timeout.
        RegistrationID worker2 = new RegistrationID();
        ReleaseRecordingShuffleWorkerGateway shuffleWorkerGateway2 =
                new ReleaseRecordingShuffleWorkerGateway();
        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker2"),
                worker2,
                shuffleWorkerGateway2,
                "xx",
                12345);
        assignmentTracker.synchronizeWorkerDataPartitions(
                worker2, Collections.singletonList(new DataPartitionStatus(jobId, coordinate)));
        assertEquals(0, assignmentTracker.getWorkers().get(worker1).getDataPartitions().size());
        assertEquals(1, assignmentTracker.getWorkers().get(worker2).getDataPartitions().size());
        assertEquals(1, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        assertEquals(
                worker2,
                assignmentTracker
                        .getJobs()
                        .get(jobId)
                        .getDataPartitions()
                        .get(coordinate)
                        .getRegistrationID());

        // Now the first worker timeout, it should not affect the current status
        assignmentTracker.unregisterWorker(worker1);
        assertEquals(1, assignmentTracker.getWorkers().get(worker2).getDataPartitions().size());
        assertEquals(1, assignmentTracker.getJobs().get(jobId).getDataPartitions().size());
        assertEquals(
                worker2,
                assignmentTracker
                        .getJobs()
                        .get(jobId)
                        .getDataPartitions()
                        .get(coordinate)
                        .getRegistrationID());

        assignmentTracker.unregisterJob(jobId);
        assertThat(
                shuffleWorkerGateway2.getReleasedPartitions(),
                containsInAnyOrder(Triple.of(jobId, dataSetId, dataPartitionId)));
    }

    @Test
    public void testInitPartitionPlacementStrategy() {
        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        PartitionPlacementStrategy defaultStrategy =
                PartitionPlacementStrategyLoader.loadPlacementStrategyFactory(configuration);
        assertTrue(defaultStrategy instanceof RoundRobinPlacementStrategy);

        assertRightPlacementStrategy(
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME);
        assertRightPlacementStrategy(
                PartitionPlacementStrategyLoader.RANDOM_PLACEMENT_STRATEGY_NAME);
        assertRightPlacementStrategy(
                PartitionPlacementStrategyLoader.ROUND_ROBIN_PLACEMENT_STRATEGY_NAME);
    }

    private static void assertRightPlacementStrategy(String strategyName) {
        Configuration configuration = new Configuration();
        configuration.setString(ManagerOptions.PARTITION_PLACEMENT_STRATEGY, strategyName);
        PartitionPlacementStrategy strategy =
                PartitionPlacementStrategyLoader.loadPlacementStrategyFactory(configuration);
        switch (strategyName) {
            case PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME:
                assertTrue(strategy instanceof MinNumberPlacementStrategy);
                break;
            case PartitionPlacementStrategyLoader.RANDOM_PLACEMENT_STRATEGY_NAME:
                assertTrue(strategy instanceof RandomPlacementStrategy);
                break;
            case PartitionPlacementStrategyLoader.ROUND_ROBIN_PLACEMENT_STRATEGY_NAME:
                assertTrue(strategy instanceof RoundRobinPlacementStrategy);
                break;
            default:
                fail();
        }
    }

    @Test(expected = ShuffleResourceAllocationException.class)
    public void testRequestShuffleResource() throws Exception {
        JobID jobId = randomJobId();
        Configuration configuration = new Configuration();
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);
        RegistrationID worker1 = new RegistrationID();
        registerWorkerToTracker(
                assignmentTracker,
                new InstanceID("worker1"),
                worker1,
                new EmptyShuffleWorkerGateway(),
                "worker1",
                1024);

        try {
            assignmentTracker.requestShuffleResource(
                    jobId, randomDataSetId(), randomMapPartitionId(), 2, partitionFactory, null);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        Map<String, UsableStorageSpaceInfo> usableSpace = new HashMap<>();
        usableSpace.put(partitionFactory, UsableStorageSpaceInfo.ZERO_USABLE_SPACE);
        assignmentTracker.getWorkers().get(worker1).updateStorageUsableSpace(usableSpace);
        assignmentTracker.requestShuffleResource(
                jobId, randomDataSetId(), randomMapPartitionId(), 2, partitionFactory, null);
    }

    // ------------------------------- Utilities ----------------------------------------------

    private AssignmentTrackerImpl createAssignmentTracker(
            JobID jobId,
            RegistrationID workerRegistrationId,
            InstanceID workerInstanceID,
            ShuffleWorkerGateway gateway) {
        Configuration configuration = new Configuration();
        configuration.setMemorySize(StorageOptions.STORAGE_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        registerWorkerToTracker(
                assignmentTracker,
                workerInstanceID,
                workerRegistrationId,
                gateway,
                "worker1",
                1024);
        return assignmentTracker;
    }

    private static void registerWorkerToTracker(
            AssignmentTrackerImpl assignmentTracker,
            InstanceID workerID,
            RegistrationID registrationID,
            ShuffleWorkerGateway gateway,
            String workerAddr,
            int dataPort) {
        assignmentTracker.registerWorker(workerID, registrationID, gateway, workerAddr, dataPort);
        Map<String, UsableStorageSpaceInfo> usableSpace = new HashMap<>();
        usableSpace.put(partitionFactory, UsableStorageSpaceInfo.INFINITE_USABLE_SPACE);
        assignmentTracker.getWorkers().get(registrationID).updateStorageUsableSpace(usableSpace);
    }

    private static class ReleaseRecordingShuffleWorkerGateway extends EmptyShuffleWorkerGateway {
        private final List<Triple<JobID, DataSetID, DataPartitionID>> releasedPartitions =
                new ArrayList<>();

        private final List<Triple<JobID, DataSetID, DataPartitionID>> releaseMetaPartitions =
                new ArrayList<>();

        @Override
        public CompletableFuture<Acknowledge> releaseDataPartition(
                JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
            releasedPartitions.add(Triple.of(jobID, dataSetID, dataPartitionID));
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        @Override
        public CompletableFuture<Acknowledge> removeReleasedDataPartitionMeta(
                JobID jobID, DataSetID dataSetID, DataPartitionID dataPartitionID) {
            releaseMetaPartitions.add(Triple.of(jobID, dataSetID, dataPartitionID));
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        public List<Triple<JobID, DataSetID, DataPartitionID>> getReleasedPartitions() {
            return releasedPartitions;
        }

        public List<Triple<JobID, DataSetID, DataPartitionID>> getReleaseMetaPartitions() {
            return releaseMetaPartitions;
        }

        public void reset() {
            releasedPartitions.clear();
            releaseMetaPartitions.clear();
        }
    }
}
