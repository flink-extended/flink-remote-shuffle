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
import com.alibaba.flink.shuffle.coordinator.manager.DataPartitionCoordinate;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.PARTITION_FACTORY_CLASS;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.createAssignmentTrackerImpl;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.expectedNoAvailableWorkersException;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.registerWorkerToTracker;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.selectWorkerWithEnoughSpace;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomDataSetId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomJobId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomMapPartitionId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link RandomPlacementStrategy}. */
public class RandomPlacementStrategyTest {
    private JobID jobId;

    private InstanceID workerInstance1;

    private InstanceID workerInstance2;

    private AssignmentTrackerImpl assignmentTracker;

    @Before
    public void start() {
        jobId = randomJobId();
        assignmentTracker =
                createAssignmentTrackerImpl(
                        PartitionPlacementStrategyLoader.RANDOM_PLACEMENT_STRATEGY_NAME,
                        MemorySize.ZERO);
        assignmentTracker.registerJob(jobId);

        workerInstance1 = new InstanceID("worker1");
        workerInstance2 = new InstanceID("worker2");

        registerWorkerToTracker(assignmentTracker, workerInstance1, "worker1", 1024);
        registerWorkerToTracker(assignmentTracker, workerInstance2, "worker2", 1025);
    }

    @Test
    public void testSelectRightWorker() throws ShuffleResourceAllocationException {
        List<ShuffleResource> shuffleResources = new ArrayList<>();
        Set<String> workerNames = new HashSet<>();

        for (int i = 0; i < 100; i++) {
            MapPartitionID dataPartitionId = randomMapPartitionId();
            ShuffleResource shuffleResource =
                    assignmentTracker.requestShuffleResource(
                            jobId,
                            randomDataSetId(),
                            dataPartitionId,
                            2,
                            PARTITION_FACTORY_CLASS,
                            null);
            shuffleResources.add(shuffleResource);
            workerNames.add(shuffleResource.getMapPartitionLocation().getWorkerAddress());
        }
        assertEquals(100, shuffleResources.size());
        assertTrue(workerNames.contains("worker1") && workerNames.contains("worker2"));
        Map<DataPartitionCoordinate, InstanceID> distribution =
                assignmentTracker.getDataPartitionDistribution(jobId);

        int numWorker1 = 0;
        int numWorker2 = 0;
        for (InstanceID instanceID : distribution.values()) {
            if (instanceID.equals(workerInstance1)) {
                numWorker1++;
            } else if (instanceID.equals(workerInstance2)) {
                numWorker2++;
            }
        }
        assertEquals(100, numWorker1 + numWorker2);
        assertTrue(numWorker1 > 1 && numWorker2 > 1);
    }

    @Test
    public void testSelectWorkerWithEnoughSpace() throws ShuffleResourceAllocationException {
        selectWorkerWithEnoughSpace(
                PartitionPlacementStrategyLoader.RANDOM_PLACEMENT_STRATEGY_NAME,
                "worker1",
                "worker2",
                "worker3",
                null);
    }

    @Test(expected = ShuffleResourceAllocationException.class)
    public void testNoAvailableWorkersException() throws ShuffleResourceAllocationException {
        expectedNoAvailableWorkersException(
                PartitionPlacementStrategyLoader.RANDOM_PLACEMENT_STRATEGY_NAME,
                "worker1",
                "worker2",
                null);
    }
}
