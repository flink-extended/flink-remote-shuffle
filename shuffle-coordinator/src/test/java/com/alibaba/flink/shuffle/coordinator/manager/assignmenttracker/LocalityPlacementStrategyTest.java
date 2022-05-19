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

import com.alibaba.flink.shuffle.common.config.MemorySize;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.PARTITION_FACTORY_CLASS;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.createAssignmentTrackerImpl;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.expectedNoAvailableWorkersException;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.registerWorkerToTracker;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.selectWorkerWithEnoughSpace;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomDataSetId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomJobId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomMapPartitionId;
import static org.junit.Assert.assertEquals;

/** Tests for {@link LocalityPlacementStrategy}. */
public class LocalityPlacementStrategyTest {
    private JobID jobId;

    private AssignmentTrackerImpl assignmentTracker;

    @Before
    public void start() {
        jobId = randomJobId();
        assignmentTracker =
                createAssignmentTrackerImpl(
                        PartitionPlacementStrategyLoader.LOCALITY_PLACEMENT_STRATEGY_NAME,
                        MemorySize.parse("1b"));
        assignmentTracker.registerJob(jobId);

        InstanceID workerInstance1 = new InstanceID("localhost");
        InstanceID workerInstance2 = new InstanceID("remote");

        registerWorkerToTracker(assignmentTracker, workerInstance1, "localhost", 1024);
        registerWorkerToTracker(assignmentTracker, workerInstance2, "remote", 1025);
    }

    @Test
    public void testSelectRightWorker() throws Exception {
        for (int i = 0; i < 100; i++) {
            MapPartitionID dataPartitionId = randomMapPartitionId();
            ShuffleResource shuffleResource =
                    assignmentTracker.requestShuffleResource(
                            jobId,
                            randomDataSetId(),
                            dataPartitionId,
                            2,
                            PARTITION_FACTORY_CLASS,
                            "localhost");
            assertEquals("localhost", shuffleResource.getMapPartitionLocation().getWorkerAddress());
        }

        Map<String, StorageSpaceInfo> storageSpaceInfos = new HashMap<>();
        storageSpaceInfos.put(PARTITION_FACTORY_CLASS, StorageSpaceInfo.ZERO_STORAGE_SPACE);
        for (WorkerStatus worker : assignmentTracker.getWorkers().values()) {
            if (worker.getWorkerAddress().equals("localhost")) {
                worker.updateStorageSpaceInfo(storageSpaceInfos);
            }
        }

        for (int i = 0; i < 100; i++) {
            MapPartitionID dataPartitionId = randomMapPartitionId();
            ShuffleResource shuffleResource =
                    assignmentTracker.requestShuffleResource(
                            jobId,
                            randomDataSetId(),
                            dataPartitionId,
                            2,
                            PARTITION_FACTORY_CLASS,
                            "localhost");
            assertEquals("remote", shuffleResource.getMapPartitionLocation().getWorkerAddress());
        }
    }

    @Test
    public void testSelectWorkerWithEnoughSpace() throws Exception {
        selectWorkerWithEnoughSpace(
                PartitionPlacementStrategyLoader.LOCALITY_PLACEMENT_STRATEGY_NAME,
                "localhost",
                "remote1",
                "remote2",
                "localhost");
    }

    @Test(expected = ShuffleResourceAllocationException.class)
    public void testNoAvailableWorkersException() throws Exception {
        expectedNoAvailableWorkersException(
                PartitionPlacementStrategyLoader.LOCALITY_PLACEMENT_STRATEGY_NAME,
                "localhost",
                "remote",
                "localhost");
    }
}
