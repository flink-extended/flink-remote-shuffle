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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.core.ids.JobID;

import java.io.Serializable;
import java.util.Objects;

/** The DataPartition's state. */
public class DataPartitionStatus implements Serializable, Cloneable {

    private static final long serialVersionUID = 1912308953069806999L;

    private final JobID jobId;

    private final DataPartitionCoordinate coordinate;

    private boolean isReleasing;

    public DataPartitionStatus(JobID jobId, DataPartitionCoordinate coordinate) {
        this(jobId, coordinate, false);
    }

    public DataPartitionStatus(
            JobID jobId, DataPartitionCoordinate coordinate, boolean isReleasing) {
        this.jobId = jobId;
        this.coordinate = coordinate;
        this.isReleasing = isReleasing;
    }

    public JobID getJobId() {
        return jobId;
    }

    public DataPartitionCoordinate getCoordinate() {
        return coordinate;
    }

    public boolean isReleasing() {
        return isReleasing;
    }

    public void setReleasing(boolean releasing) {
        isReleasing = releasing;
    }

    @Override
    public DataPartitionStatus clone() {
        return new DataPartitionStatus(jobId, coordinate, isReleasing);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DataPartitionStatus that = (DataPartitionStatus) o;
        return isReleasing == that.isReleasing
                && Objects.equals(jobId, that.jobId)
                && Objects.equals(coordinate, that.coordinate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, coordinate, isReleasing);
    }
}
