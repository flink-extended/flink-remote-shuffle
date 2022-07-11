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
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.ReducePartitionMeta;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/** {@link DataPartitionMeta} of {@link LocalFileReducePartition}. */
public class LocalFileReducePartitionMeta extends ReducePartitionMeta {
    private static final long serialVersionUID = -2329943525903450813L;

    /** Meta of the corresponding {@link LocalReducePartitionFile}. */
    private final LocalReducePartitionFileMeta fileMeta;

    public LocalFileReducePartitionMeta(
            JobID jobID,
            DataSetID dataSetID,
            ReducePartitionID partitionID,
            LocalReducePartitionFileMeta fileMeta,
            StorageMeta storageMeta) {
        super(jobID, dataSetID, partitionID, storageMeta);

        CommonUtils.checkArgument(fileMeta != null, "Must be not null.");
        this.fileMeta = fileMeta;
    }

    /**
     * Reconstructs the {@link DataPartitionMeta} from {@link DataInput} when recovering from
     * failure.
     */
    public static LocalFileReducePartitionMeta readFrom(
            DataInput dataInput, DataPartitionFactory partitionFactory) throws IOException {
        JobID jobID = JobID.readFrom(dataInput);
        DataSetID dataSetID = DataSetID.readFrom(dataInput);
        ReducePartitionID partitionID = ReducePartitionID.readFrom(dataInput);

        StorageMeta storageMeta = StorageMeta.readFrom(dataInput, partitionFactory);
        LocalReducePartitionFileMeta fileMeta = LocalReducePartitionFileMeta.readFrom(dataInput);

        return new LocalFileReducePartitionMeta(
                jobID, dataSetID, partitionID, fileMeta, storageMeta);
    }

    @Override
    public String getPartitionFactoryClassName() {
        return LocalFileReducePartitionFactory.class.getName();
    }

    public LocalReducePartitionFileMeta getPartitionFileMeta() {
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

        if (!(that instanceof LocalFileReducePartitionMeta)) {
            return false;
        }

        LocalFileReducePartitionMeta partitionMeta = (LocalFileReducePartitionMeta) that;
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
        return "LocalFileReducePartitionMeta{"
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
