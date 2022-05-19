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

package com.alibaba.flink.shuffle.kubernetes.operator.crd;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/** Test for {@link RemoteShuffleApplicationStatus}. */
public class RemoteShuffleApplicationStatusTest {

    @Test
    public void testToString() {
        RemoteShuffleApplicationStatus status = new RemoteShuffleApplicationStatus(1, 2, 3, 4);
        assertEquals(
                status.toString(),
                "RemoteShuffleApplicationStatus(readyShuffleManagers=1, readyShuffleWorkers=2, desiredShuffleManagers=3, desiredShuffleWorkers=4)");
    }

    @Test
    public void testEquals() {

        RemoteShuffleApplicationStatus status1 = new RemoteShuffleApplicationStatus(1, 2, 3, 4);
        RemoteShuffleApplicationStatus status2 = new RemoteShuffleApplicationStatus(1, 2, 3, 4);
        RemoteShuffleApplicationStatus status3 = new RemoteShuffleApplicationStatus(0, 2, 3, 4);
        RemoteShuffleApplicationStatus status4 = new RemoteShuffleApplicationStatus(1, 0, 3, 4);
        RemoteShuffleApplicationStatus status5 = new RemoteShuffleApplicationStatus(1, 2, 0, 4);
        RemoteShuffleApplicationStatus status6 = new RemoteShuffleApplicationStatus(1, 2, 3, 0);

        assertEquals(status1, status2);
        assertNotEquals(status1, status3);
        assertNotEquals(status1, status4);
        assertNotEquals(status1, status5);
        assertNotEquals(status1, status6);
    }
}
