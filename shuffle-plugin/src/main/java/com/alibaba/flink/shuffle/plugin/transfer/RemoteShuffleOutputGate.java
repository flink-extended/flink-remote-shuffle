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

package com.alibaba.flink.shuffle.plugin.transfer;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ShuffleWriteClient;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A transportation gate used to spill buffers from {@link ResultPartitionWriter} to remote shuffle
 * worker.
 */
public class RemoteShuffleOutputGate {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleOutputGate.class);

    /** A {@link ShuffleDescriptor} which describes shuffle meta and shuffle worker address. */
    private final RemoteShuffleDescriptor shuffleDesc;

    /** Number of subpartitions of the corresponding {@link ResultPartitionWriter}. */
    protected final int numSubs;

    /** Used to transport data to a shuffle worker. */
    private final ShuffleWriteClient shuffleWriteClient;

    /** Used to consolidate buffers. */
    private final BufferPacker bufferPacker;

    /** {@link BufferPool} provider. */
    protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Provides buffers to hold data to send online by Netty layer. */
    protected BufferPool bufferPool;

    /**
     * @param shuffleDesc Describes shuffle meta and shuffle worker address.
     * @param numSubs Number of subpartitions of the corresponding {@link ResultPartitionWriter}.
     * @param bufferPoolFactory {@link BufferPool} provider.
     * @param connectionManager Manages physical connections.
     */
    public RemoteShuffleOutputGate(
            RemoteShuffleDescriptor shuffleDesc,
            int numSubs,
            int bufferSize,
            String dataPartitionFactoryName,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            ConnectionManager connectionManager) {

        this.shuffleDesc = shuffleDesc;
        this.numSubs = numSubs;
        this.bufferPoolFactory = bufferPoolFactory;
        this.shuffleWriteClient =
                createWriteClient(bufferSize, dataPartitionFactoryName, connectionManager);
        this.bufferPacker = new BufferPacker(shuffleWriteClient::write);
    }

    /** Initialize transportation gate. */
    public void setup() throws IOException, InterruptedException {
        bufferPool = CommonUtils.checkNotNull(bufferPoolFactory.get());
        CommonUtils.checkArgument(
                bufferPool.getNumberOfRequiredMemorySegments() >= 2,
                "Too few buffers for transfer, the minimum valid required size is 2.");

        // guarantee that we have at least one buffer
        BufferUtils.reserveNumRequiredBuffers(bufferPool, 1);

        shuffleWriteClient.open();
    }

    /** Get transportation buffer pool. */
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    /** Writes a {@link Buffer} to a subpartition. */
    public void write(Buffer buffer, int subIdx) throws InterruptedException {
        bufferPacker.process(buffer, subIdx);
    }

    /**
     * Indicates the start of a region. A region of buffers guarantees the records inside are
     * completed.
     *
     * @param isBroadcast Whether it's a broadcast region.
     */
    public void regionStart(boolean isBroadcast) {
        shuffleWriteClient.regionStart(isBroadcast);
    }

    /**
     * Indicates the finish of a region. A region is always bounded by a pair of region-start and
     * region-finish.
     */
    public void regionFinish() throws InterruptedException {
        bufferPacker.drain();
        shuffleWriteClient.regionFinish();
    }

    /** Indicates the writing/spilling is finished. */
    public void finish() throws InterruptedException {
        shuffleWriteClient.finish();
    }

    /** Close the transportation gate. */
    public void close() throws IOException {
        Throwable closeException = null;
        try {
            if (bufferPool != null) {
                bufferPool.lazyDestroy();
            }
        } catch (Throwable throwable) {
            closeException = throwable;
            LOG.error("Failed to close local buffer pool.", throwable);
        }

        try {
            bufferPacker.close();
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close buffer packer.", throwable);
        }

        try {
            shuffleWriteClient.close();
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close shuffle write client.", throwable);
        }

        if (closeException != null) {
            ExceptionUtils.rethrowAsRuntimeException(closeException);
        }
    }

    /** Returns shuffle descriptor. */
    public RemoteShuffleDescriptor getShuffleDesc() {
        return shuffleDesc;
    }

    private ShuffleWriteClient createWriteClient(
            int bufferSize, String dataPartitionFactoryName, ConnectionManager connectionManager) {
        JobID jobID = shuffleDesc.getJobId();
        DataSetID dataSetID = shuffleDesc.getDataSetId();
        MapPartitionID mapID = (MapPartitionID) shuffleDesc.getDataPartitionID();
        ShuffleWorkerDescriptor swd =
                ((DefaultShuffleResource) shuffleDesc.getShuffleResource())
                        .getMapPartitionLocation();
        InetSocketAddress address =
                new InetSocketAddress(swd.getWorkerAddress(), swd.getDataPort());
        return new ShuffleWriteClient(
                address,
                jobID,
                dataSetID,
                mapID,
                numSubs,
                bufferSize,
                dataPartitionFactoryName,
                connectionManager);
    }
}
