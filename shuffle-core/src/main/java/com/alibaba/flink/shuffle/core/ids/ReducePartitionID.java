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

package com.alibaba.flink.shuffle.core.ids;

import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.ReducePartition;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;

/** ID of the {@link ReducePartition}. */
public class ReducePartitionID extends DataPartitionID {

    private static final long serialVersionUID = 5443963157496120768L;

    private final int partitionIndex;

    private final long consumerGroupID;

    @Override
    public DataPartition.DataPartitionType getPartitionType() {
        return DataPartition.DataPartitionType.REDUCE_PARTITION;
    }

    public ReducePartitionID(int partitionIndex) {
        this(partitionIndex, 0);
    }

    public ReducePartitionID(int partitionIndex, long consumerGroupID) {
        super(getBytes(partitionIndex, consumerGroupID));
        this.partitionIndex = partitionIndex;
        this.consumerGroupID = consumerGroupID;
    }

    private static byte[] getBytes(int value, long consumerGroupID) {
        byte[] bytes = new byte[4 + 8];
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(value);
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putLong(consumerGroupID);
        return bytes;
    }

    /** Deserializes and creates an {@link ReducePartitionID} from the given {@link DataInput}. */
    public static ReducePartitionID readFrom(ByteBuf byteBuf) {
        return new ReducePartitionID(byteBuf.readInt(), byteBuf.readLong());
    }

    /** Deserializes and creates an {@link ReducePartitionID} from the given {@link DataInput}. */
    public static ReducePartitionID readFrom(DataInput dataInput) throws IOException {
        return new ReducePartitionID(dataInput.readInt(), dataInput.readLong());
    }

    public int getPartitionIndex() {
        return partitionIndex;
    }

    public long getConsumerGroupID() {
        return consumerGroupID;
    }

    @Override
    public int getFootprint() {
        return 4 + 8;
    }

    @Override
    public void writeTo(ByteBuf byteBuf) {
        byteBuf.writeInt(partitionIndex);
        byteBuf.writeLong(consumerGroupID);
    }

    @Override
    public void writeTo(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(partitionIndex);
        dataOutput.writeLong(consumerGroupID);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        ReducePartitionID that = (ReducePartitionID) o;
        return partitionIndex == that.partitionIndex && consumerGroupID == that.consumerGroupID;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), partitionIndex, consumerGroupID);
    }

    @Override
    public String toString() {
        return "ReducePartitionID{"
                + "PartitionIndex="
                + partitionIndex
                + ",consumerGroupID="
                + consumerGroupID
                + '}';
    }
}
