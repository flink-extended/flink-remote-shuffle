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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.MapPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/** {@link DataPartitionMeta} of {@link LocalFileMapPartition}. */
public class LocalFileMapPartitionMeta extends MapPartitionMeta {

    private static final long serialVersionUID = -7947536484763210922L;

    /** Meta of the corresponding {@link LocalMapPartitionFile}. */
    private final LocalMapPartitionFileMeta fileMeta;

    public LocalFileMapPartitionMeta(
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID partitionID,
            LocalMapPartitionFileMeta fileMeta,
            StorageMeta storageMeta) {
        super(jobID, dataSetID, partitionID, storageMeta);

        CommonUtils.checkArgument(fileMeta != null, "Must be not null.");
        this.fileMeta = fileMeta;
    }

    /**
     * Reconstructs the {@link DataPartitionMeta} from {@link DataInput} when recovering from
     * failure.
     */
    public static LocalFileMapPartitionMeta readFrom(DataInput dataInput) throws IOException {
        JobID jobID = JobID.readFrom(dataInput);
        DataSetID dataSetID = DataSetID.readFrom(dataInput);
        MapPartitionID partitionID = MapPartitionID.readFrom(dataInput);

        StorageMeta storageMeta = StorageMeta.readFrom(dataInput);
        LocalMapPartitionFileMeta fileMeta = LocalMapPartitionFileMeta.readFrom(dataInput);

        return new LocalFileMapPartitionMeta(jobID, dataSetID, partitionID, fileMeta, storageMeta);
    }

    @Override
    public String getPartitionFactoryClassName() {
        return LocalFileMapPartitionFactory.class.getName();
    }

    public LocalMapPartitionFileMeta getPartitionFileMeta() {
        return fileMeta;
    }

    @Override
    public void writeTo(DataOutput dataOutput) throws Exception {
        jobID.writeTo(dataOutput);
        dataSetID.writeTo(dataOutput);
        partitionID.writeTo(dataOutput);
        storageMeta.writeTo(dataOutput);
        fileMeta.writeTo(dataOutput);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (!(that instanceof LocalFileMapPartitionMeta)) {
            return false;
        }

        LocalFileMapPartitionMeta partitionMeta = (LocalFileMapPartitionMeta) that;
        return Objects.equals(jobID, partitionMeta.jobID)
                && Objects.equals(dataSetID, partitionMeta.dataSetID)
                && Objects.equals(partitionID, partitionMeta.partitionID)
                && Objects.equals(storageMeta, partitionMeta.storageMeta)
                && Objects.equals(fileMeta, partitionMeta.fileMeta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobID, dataSetID, partitionID, storageMeta, fileMeta);
    }

    @Override
    public String toString() {
        return "LocalFileMapPartitionMeta{"
                + "JobID="
                + jobID
                + ", DataSetID="
                + dataSetID
                + ", PartitionID="
                + partitionID
                + ", StorageMeta="
                + storageMeta
                + ", FileMeta="
                + fileMeta
                + '}';
    }
}
