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

package com.alibaba.flink.shuffle.e2e;

import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;

import org.junit.Test;

import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.DataScale.NORMAL;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.STAGE0_NAME;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.STAGE1_NAME;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.STAGE2_NAME;
import static com.alibaba.flink.shuffle.e2e.JobForShuffleTesting.TaskStat;

/** Test for scenario of TM lost. */
public class TMLostE2ETest extends AbstractInstableE2ETest {

    @Test
    public void testTMLostAndTaskFailureWhenShuffleWrite() throws Exception {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster, NORMAL);
            planKillingTaskManagerInRunningTask(job, STAGE1_NAME, 0, 0);
            job.planFailingARunningTask(STAGE1_NAME, 0, 1);
            job.run();

            flinkCluster.checkResourceReleased();
            shuffleCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            ExceptionUtils.rethrowException(t);
        }
    }

    @Test
    public void testTMLostWithShuffleWrite() throws Exception {
        testTMLost(STAGE0_NAME);
    }

    @Test
    public void testTMLostWithShuffleWriteAndRead() throws Exception {
        testTMLost(STAGE1_NAME);
    }

    @Test
    public void testTMLostWithShuffleRead() throws Exception {
        testTMLost(STAGE2_NAME);
    }

    private void testTMLost(String stageName) throws Exception {
        try {
            JobForShuffleTesting job = new JobForShuffleTesting(flinkCluster, NORMAL);
            planKillingTaskManagerInRunningTask(job, stageName, 0, 0);

            job.run();

            // other stages' tasks should not restart.
            flinkCluster.checkResourceReleased();
            shuffleCluster.checkResourceReleased();
        } catch (Throwable t) {
            flinkCluster.printProcessLog();
            shuffleCluster.printProcessLog();
            ExceptionUtils.rethrowException(t);
        }
    }

    private void planKillingTaskManagerInRunningTask(
            JobForShuffleTesting job, String stageName, int taskID, int attemptID)
            throws Exception {
        job.planOperation(
                stageName,
                taskID,
                attemptID,
                TaskStat.RUNNING,
                info -> Runtime.getRuntime().exec("kill -9 " + info.getProcessID()));
    }
}
