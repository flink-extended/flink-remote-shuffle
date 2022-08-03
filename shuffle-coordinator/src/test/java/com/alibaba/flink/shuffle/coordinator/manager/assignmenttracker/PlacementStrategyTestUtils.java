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
import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.utils.EmptyShuffleWorkerGateway;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomDataSetId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomJobId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomMapPartitionId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** This class contains auxiliary methods for unit tests of {@link PartitionPlacementStrategy}. */
public class PlacementStrategyTestUtils {

    static final String PARTITION_FACTORY_CLASS =
            "com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory";

    static AssignmentTrackerImpl createAssignmentTrackerImpl(
            String placementStrategyName, MemorySize reservedBytes) {
        Configuration configuration = new Configuration();
        configuration.setString(ManagerOptions.PARTITION_PLACEMENT_STRATEGY, placementStrategyName);
        configuration.setMemorySize(StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, reservedBytes);
        return new AssignmentTrackerImpl(configuration);
    }

    static void registerWorkerToTracker(
            AssignmentTrackerImpl assignmentTracker,
            InstanceID workerInstanceID,
            String workerAddr,
            int dataPort) {
        registerWorkerToTracker(
                assignmentTracker, workerInstanceID, new RegistrationID(), workerAddr, dataPort);
    }

    static void registerWorkerToTracker(
            AssignmentTrackerImpl assignmentTracker,
            InstanceID workerInstanceID,
            RegistrationID registrationID,
            String workerAddr,
            int dataPort) {
        assignmentTracker.registerWorker(
                workerInstanceID,
                registrationID,
                new EmptyShuffleWorkerGateway(),
                workerAddr,
                dataPort);
        Map<String, StorageSpaceInfo> storageSpaceInfos = new HashMap<>();
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, StorageSpaceInfo.INFINITE_STORAGE_SPACE);
        assignmentTracker
                .getWorkers()
                .get(registrationID)
                .updateStorageSpaceInfo(storageSpaceInfos);
    }

    static void selectWorkerWithEnoughSpace(
            String placementStrategyName,
            String worker1,
            String worker2,
            String worker3,
            String taskLocation)
            throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        Configuration configuration = new Configuration();
        configuration.setString(ManagerOptions.PARTITION_PLACEMENT_STRATEGY, placementStrategyName);
        configuration.setMemorySize(
                StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, MemorySize.parse("1k"));
        configuration.setMemorySize(
                StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES, MemorySize.parse("1k"));
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        InstanceID workerInstance1 = new InstanceID(worker1);
        InstanceID workerInstance2 = new InstanceID(worker2);
        InstanceID workerInstance3 = new InstanceID(worker3);
        RegistrationID registrationID1 = new RegistrationID();
        RegistrationID registrationID2 = new RegistrationID();
        RegistrationID registrationID3 = new RegistrationID();
        assignmentTracker.registerWorker(
                workerInstance1, registrationID1, new EmptyShuffleWorkerGateway(), worker1, 1024);
        assignmentTracker.registerWorker(
                workerInstance2, registrationID2, new EmptyShuffleWorkerGateway(), worker2, 1025);
        assignmentTracker.registerWorker(
                workerInstance3, registrationID3, new EmptyShuffleWorkerGateway(), worker3, 1026);

        assertNotNull(assignmentTracker.getWorkers().get(registrationID1));
        Map<String, StorageSpaceInfo> storageSpaceInfos = new HashMap<>();
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, new StorageSpaceInfo(1025, 0, 1023, 1023));
        assignmentTracker
                .getWorkers()
                .get(registrationID1)
                .updateStorageSpaceInfo(storageSpaceInfos);

        assertNotNull(assignmentTracker.getWorkers().get(registrationID2));
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, new StorageSpaceInfo(1024, 0, 0, 0));
        assignmentTracker
                .getWorkers()
                .get(registrationID2)
                .updateStorageSpaceInfo(storageSpaceInfos);

        assertNotNull(assignmentTracker.getWorkers().get(registrationID2));
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, new StorageSpaceInfo(0, 0, 1025, 1025));
        assignmentTracker
                .getWorkers()
                .get(registrationID2)
                .updateStorageSpaceInfo(storageSpaceInfos);

        List<ShuffleResource> shuffleResources = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            MapPartitionID dataPartitionId = randomMapPartitionId();
            ShuffleResource shuffleResource =
                    assignmentTracker.requestShuffleResource(
                            jobId,
                            randomDataSetId(),
                            dataPartitionId,
                            2,
                            0,
                            PARTITION_FACTORY_CLASS,
                            taskLocation);
            shuffleResources.add(shuffleResource);
        }

        assertEquals(100, shuffleResources.size());
        Map<DataPartitionCoordinate, InstanceID> distribution =
                assignmentTracker.getDataPartitionDistribution(jobId);

        int numWorker1 = 0;
        int numWorker2 = 0;
        int numWorker3 = 0;
        for (InstanceID instanceID : distribution.values()) {
            if (instanceID.equals(workerInstance1)) {
                numWorker1++;
            } else if (instanceID.equals(workerInstance2)) {
                numWorker2++;
            } else if (instanceID.equals(workerInstance3)) {
                numWorker3++;
            }
        }
        assertTrue(numWorker1 == 100 && numWorker2 == 0 && numWorker3 == 0);
    }

    static void expectedNoAvailableWorkersException(
            String placementStrategyName, String worker1, String worker2, String taskLocation)
            throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();
        Configuration configuration = new Configuration();
        configuration.setString(ManagerOptions.PARTITION_PLACEMENT_STRATEGY, placementStrategyName);
        configuration.setMemorySize(
                StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, MemorySize.parse("1k"));
        configuration.setMemorySize(
                StorageOptions.STORAGE_MAX_USABLE_SPACE_BYTES, MemorySize.parse("1k"));
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        InstanceID workerInstance1 = new InstanceID(worker1);
        InstanceID workerInstance2 = new InstanceID(worker2);
        RegistrationID registrationID1 = new RegistrationID();
        RegistrationID registrationID2 = new RegistrationID();
        assignmentTracker.registerWorker(
                workerInstance1, registrationID1, new EmptyShuffleWorkerGateway(), worker1, 1024);
        assignmentTracker.registerWorker(
                workerInstance2, registrationID2, new EmptyShuffleWorkerGateway(), worker2, 1025);

        assertNotNull(assignmentTracker.getWorkers().get(registrationID1));
        Map<String, StorageSpaceInfo> storageSpaceInfos = new HashMap<>();
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, new StorageSpaceInfo(1023, 0, 0, 0));
        assignmentTracker
                .getWorkers()
                .get(registrationID1)
                .updateStorageSpaceInfo(storageSpaceInfos);

        assertNotNull(assignmentTracker.getWorkers().get(registrationID2));
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, new StorageSpaceInfo(0, 0, 1025, 1025));
        assignmentTracker
                .getWorkers()
                .get(registrationID2)
                .updateStorageSpaceInfo(storageSpaceInfos);

        try {
            assignmentTracker.requestShuffleResource(
                    jobId,
                    randomDataSetId(),
                    randomMapPartitionId(),
                    2,
                    0,
                    PARTITION_FACTORY_CLASS,
                    taskLocation);
        } catch (ShuffleResourceAllocationException e) {
            assertTrue(
                    ExceptionUtils.findThrowable(
                                    e,
                                    exception ->
                                            exception
                                                    .getMessage()
                                                    .contains(
                                                            "maybe all workers have been filtered out"))
                            .isPresent());
            throw e;
        }
    }
}
