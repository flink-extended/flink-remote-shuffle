/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;

import java.io.Serializable;
import java.util.Objects;

/**
 * The coordinate of a data partition. A data partition could be fully identified by the DataSetID
 * and the DataPartitionID.
 */
public class DataPartitionCoordinate implements Serializable {

    private static final long serialVersionUID = 6488556324861837102L;

    private final DataSetID dataSetId;

    private final DataPartitionID dataPartitionId;

    public DataPartitionCoordinate(DataSetID dataSetId, DataPartitionID dataPartitionId) {
        this.dataSetId = dataSetId;
        this.dataPartitionId = dataPartitionId;
    }

    public DataSetID getDataSetId() {
        return dataSetId;
    }

    public DataPartitionID getDataPartitionId() {
        return dataPartitionId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataPartitionCoordinate that = (DataPartitionCoordinate) o;
        return Objects.equals(dataSetId, that.dataSetId)
                && Objects.equals(dataPartitionId, that.dataPartitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dataSetId, dataPartitionId);
    }

    @Override
    public String toString() {
        return "DataPartitionCoordinate{"
                + "dataSetId="
                + dataSetId
                + ", dataPartitionId="
                + dataPartitionId
                + '}';
    }
}
