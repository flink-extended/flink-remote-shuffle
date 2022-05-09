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
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;

import org.junit.Test;

import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.PARTITION_FACTORY_CLASS;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.expectedNoAvailableWorkersException;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.registerWorkerToTracker;
import static com.alibaba.flink.shuffle.coordinator.manager.assignmenttracker.PlacementStrategyTestUtils.selectWorkerWithEnoughSpace;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomDataSetId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomJobId;
import static com.alibaba.flink.shuffle.coordinator.utils.RandomIDUtils.randomMapPartitionId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link MinNumberPlacementStrategy}. */
public class MinNumberPlacementStrategyTest {

    @Test
    public void testSelectRightWorker() throws ShuffleResourceAllocationException {
        JobID jobId = randomJobId();

        Configuration configuration = new Configuration();
        configuration.setString(
                ManagerOptions.PARTITION_PLACEMENT_STRATEGY,
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME);
        configuration.setMemorySize(
                StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES, MemorySize.ZERO);
        AssignmentTrackerImpl assignmentTracker = new AssignmentTrackerImpl(configuration);
        assignmentTracker.registerJob(jobId);

        // Registers two workers
        RegistrationID worker1 = new RegistrationID();
        RegistrationID worker2 = new RegistrationID();

        registerWorkerToTracker(
                assignmentTracker, new InstanceID("worker1"), worker1, "worker1", 1024);
        registerWorkerToTracker(
                assignmentTracker, new InstanceID("worker2"), worker2, "worker2", 1026);

        MapPartitionID dataPartitionId1 = randomMapPartitionId();
        MapPartitionID dataPartitionId2 = randomMapPartitionId();

        ShuffleResource shuffleResource1 =
                assignmentTracker.requestShuffleResource(
                        jobId,
                        randomDataSetId(),
                        dataPartitionId1,
                        2,
                        PARTITION_FACTORY_CLASS,
                        null);
        String workerAddress1 = shuffleResource1.getMapPartitionLocation().getWorkerAddress();
        ShuffleResource shuffleResource2 =
                assignmentTracker.requestShuffleResource(
                        jobId,
                        randomDataSetId(),
                        dataPartitionId2,
                        2,
                        PARTITION_FACTORY_CLASS,
                        null);
        String workerAddress2 = shuffleResource2.getMapPartitionLocation().getWorkerAddress();

        assertTrue(workerAddress1.equals("worker1") || workerAddress1.equals("worker2"));
        if (workerAddress1.equals("worker1")) {
            assertEquals("worker2", workerAddress2);
        } else {
            assertEquals("worker1", workerAddress2);
        }
    }

    @Test
    public void testSelectWorkerWithEnoughSpace() throws ShuffleResourceAllocationException {
        selectWorkerWithEnoughSpace(
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME,
                "worker1",
                "worker2",
                "worker3",
                null);
    }

    @Test(expected = ShuffleResourceAllocationException.class)
    public void testNoAvailableWorkersException() throws ShuffleResourceAllocationException {
        expectedNoAvailableWorkersException(
                PartitionPlacementStrategyLoader.MIN_NUM_PLACEMENT_STRATEGY_NAME,
                "worker1",
                "worker2",
                null);
    }
}
