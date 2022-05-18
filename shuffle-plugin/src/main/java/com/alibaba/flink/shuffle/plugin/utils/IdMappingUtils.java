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

package com.alibaba.flink.shuffle.plugin.utils;

import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.buffer.Unpooled;

/** Processes the mapping from flink ids to the remote shuffle ids. */
public class IdMappingUtils {

    public static JobID fromFlinkJobId(AbstractID flinkJobId) {
        return new JobID(flinkJobId.getBytes());
    }

    public static DataSetID fromFlinkDataSetId(IntermediateDataSetID flinkDataSetId) {
        return new DataSetID(flinkDataSetId.getBytes());
    }

    public static MapPartitionID fromFlinkResultPartitionID(ResultPartitionID resultPartitionID) {
        ByteBuf byteBuf = Unpooled.buffer();
        resultPartitionID.getPartitionId().writeTo(byteBuf);
        resultPartitionID.getProducerId().writeTo(byteBuf);

        byte[] bytes = new byte[byteBuf.readableBytes()];
        byteBuf.readBytes(bytes);
        byteBuf.release();

        return new MapPartitionID(bytes);
    }

    public static ResultPartitionID fromMapPartitionID(MapPartitionID mapPartitionID) {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(mapPartitionID.getId());

        IntermediateResultPartitionID partitionID =
                IntermediateResultPartitionID.fromByteBuf(byteBuf);
        ExecutionAttemptID attemptID = ExecutionAttemptID.fromByteBuf(byteBuf);
        byteBuf.release();

        return new ResultPartitionID(partitionID, attemptID);
    }
}
