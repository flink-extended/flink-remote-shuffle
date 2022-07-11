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
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.storage.MapPartition;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.plugin.utils.GateUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ShuffleWriteClient;

import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

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

    /** Whether the data partition type is {@link MapPartition}. */
    private final boolean isMapPartition;

    /** Number of partitions in a {@link ConsumedPartitionGroup}. */
    private final int numMapsInGroup;

    /** Used to transport data to a shuffle worker. */
    private final Map<Integer, ShuffleWriteClient> shuffleWriteClients = new HashMap<>();

    /** Used to consolidate buffers. */
    private final Map<Integer, BufferPacker> bufferPackers = new HashMap<>();

    /** {@link BufferPool} provider. */
    protected final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Lock to wait the writing finish commitments of all the {@link ShuffleWriteClient}s. */
    protected final Object lock = new Object();

    /** Provides buffers to hold data to send online by Netty layer. */
    protected BufferPool bufferPool;

    /**
     * A queue to store the {@link ShuffleWriteClient} to send buffers in a {@link SortBuffer}, and
     * the data in the sort buffer is ready. When receiving credits from the server side, the
     * corresponding {@link ShuffleWriteClient} is polled to write data.
     */
    private final BlockingQueue<ShuffleWriteClient> writingClients = new LinkedBlockingQueue<>();

    /**
     * Number of waiting finish commitments from server side. When all finish commitments are
     * received, the {@link RemoteShuffleOutputGate} can be finished safely.
     */
    @GuardedBy("lock")
    private int numWaitingFinishCommit;

    /**
     * Whether the {@link RemoteShuffleOutputGate} is waiting finish commitments from server side.
     */
    @GuardedBy("lock")
    private boolean isWaitingFinishCommit;

    /** Whether the {@link RemoteShuffleOutputGate} is released. */
    @GuardedBy("lock")
    private boolean isReleased;

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
        this.isMapPartition = shuffleDesc.isMapPartition();
        this.numMapsInGroup = shuffleDesc.getNumMaps();
        initShuffleWriteClients(bufferSize, dataPartitionFactoryName, connectionManager);
    }

    boolean isMapPartition() {
        return isMapPartition;
    }

    ShuffleWriteClient takeWritingClient() throws InterruptedException {
        return writingClients.take();
    }

    /** Initialize transportation gate. */
    public void setup() throws IOException, InterruptedException {
        bufferPool = checkNotNull(bufferPoolFactory.get());
        CommonUtils.checkArgument(
                bufferPool.getNumberOfRequiredMemorySegments() >= 2,
                "Too few buffers for transfer, the minimum valid required size is 2.");

        // guarantee that we have at least one buffer
        BufferUtils.reserveNumRequiredBuffers(bufferPool, 1);
        for (ShuffleWriteClient writeClient : shuffleWriteClients.values()) {
            writeClient.open();
        }
    }

    /** Get transportation buffer pool. */
    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public Map<Integer, ShuffleWriteClient> getShuffleWriteClients() {
        return shuffleWriteClients;
    }

    /** Writes a {@link Buffer} to a subpartition. */
    public void write(Buffer buffer, int subIdx) throws InterruptedException {
        if (isMapPartition) {
            checkState(bufferPackers.size() == 1, "Wrong buffer packer size.");
            for (BufferPacker bufferPacker : bufferPackers.values()) {
                bufferPacker.process(buffer, subIdx);
            }
        } else {
            BufferPacker bufferPacker = bufferPackers.get(subIdx);
            checkState(bufferPacker != null, "Buffer packer must not be null.");
            if ((shuffleWriteClients.get(subIdx).needMoreThanOneBuffer()
                            && !shuffleWriteClients.get(subIdx).needMoreCredits())
                    || bufferPacker.hasCachedBuffer()) {
                bufferPacker.process(buffer, subIdx);
            } else {
                bufferPacker.writeWithoutCache(buffer, subIdx);
            }
        }
    }

    /**
     * Indicates the start of a region. A region of buffers guarantees the records inside are
     * completed.
     *
     * @param isBroadcast Whether it's a broadcast region.
     */
    public void regionStart(boolean isBroadcast, SortBuffer sortBuffer, int bufferSize) {
        for (Map.Entry<Integer, ShuffleWriteClient> clientEntry : shuffleWriteClients.entrySet()) {
            int subPartitionIndex = clientEntry.getKey();
            ShuffleWriteClient shuffleWriteClient = clientEntry.getValue();
            long numSubpartitionBytes =
                    isMapPartition() ? 0 : sortBuffer.numSubpartitionBytes(subPartitionIndex);
            int numEvents = isMapPartition() ? 0 : sortBuffer.numEvents(subPartitionIndex);
            if (isEmptyDataInRegion(shuffleWriteClient, numSubpartitionBytes, numEvents)) {
                continue;
            }

            int requireCredit =
                    BufferUtils.calculateSubpartitionCredit(
                            numSubpartitionBytes, numEvents, bufferSize);
            boolean needMoreThanOneBuffer =
                    BufferUtils.needMoreThanOneBuffer(numSubpartitionBytes, bufferSize);
            shuffleWriteClient.regionStart(
                    isBroadcast, numMapsInGroup, requireCredit, needMoreThanOneBuffer);
        }
    }

    private boolean isEmptyDataInRegion(
            ShuffleWriteClient shuffleWriteClient, long numSubpartitionBytes, int numEvents) {
        boolean isEmptyDataInRegion =
                !isMapPartition() && numSubpartitionBytes == 0 && numEvents == 0;
        shuffleWriteClient.setIsEmptyDataInRegion(isEmptyDataInRegion);
        return isEmptyDataInRegion;
    }

    public void regionStart(
            boolean isBroadcast,
            int targetSubpartition,
            int requireCredit,
            boolean needMoreThanOneBuffer) {
        checkNotNull(shuffleWriteClients.get(targetSubpartition))
                .regionStart(isBroadcast, numMapsInGroup, requireCredit, needMoreThanOneBuffer);
    }

    /**
     * Indicates the finish of a region. A region is always bounded by a pair of region-start and
     * region-finish.
     */
    public void regionFinish() throws InterruptedException {
        for (BufferPacker bufferPacker : bufferPackers.values()) {
            bufferPacker.drain();
        }

        for (ShuffleWriteClient shuffleWriteClient : shuffleWriteClients.values()) {
            if (shuffleWriteClient.isEmptyDataInRegion()) {
                continue;
            }
            shuffleWriteClient.regionFinish();
        }
    }

    public void regionFinish(int targetSubpartition) throws InterruptedException {
        if (shuffleWriteClients.get(targetSubpartition).isEmptyDataInRegion()) {
            return;
        }
        bufferPackers.get(targetSubpartition).drain();
        shuffleWriteClients.get(targetSubpartition).regionFinish();
    }

    /** Indicates the writing/spilling is finished. */
    public void finish() throws InterruptedException {
        numWaitingFinishCommit = shuffleWriteClients.size();
        for (ShuffleWriteClient writeClient : shuffleWriteClients.values()) {
            writeClient.finish(this::decWaitingFinishCommitListener);
        }
        synchronized (lock) {
            if (numWaitingFinishCommit > 0 && !isReleased) {
                isWaitingFinishCommit = true;
                lock.wait();
            }
        }
        writingClients.clear();
    }

    public void decWaitingFinishCommitListener() {
        synchronized (lock) {
            numWaitingFinishCommit--;
            if (numWaitingFinishCommit == 0) {
                lock.notifyAll();
                isWaitingFinishCommit = false;
            }
        }
        checkState(numWaitingFinishCommit >= 0, "Wrong number of waiting clients.");
    }

    void release() {
        synchronized (lock) {
            isReleased = true;
            if (isWaitingFinishCommit) {
                lock.notifyAll();
                isWaitingFinishCommit = false;
            }
        }
        writingClients.add(GateUtils.POISON);
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
            for (BufferPacker bufferPacker : bufferPackers.values()) {
                bufferPacker.close();
            }
            bufferPackers.clear();
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close buffer packer.", throwable);
        }

        try {
            for (ShuffleWriteClient shuffleWriteClient : shuffleWriteClients.values()) {
                shuffleWriteClient.close();
            }
            shuffleWriteClients.clear();
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close shuffle write client.", throwable);
        }

        try {
            while (!writingClients.isEmpty()) {
                ShuffleWriteClient shuffleWriteClient = writingClients.poll();
                if (shuffleWriteClient != null) {
                    shuffleWriteClient.close();
                }
            }
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close writing shuffle write client.", throwable);
        }

        if (closeException != null) {
            ExceptionUtils.rethrowAsRuntimeException(closeException);
        }
    }

    /** Returns shuffle descriptor. */
    public RemoteShuffleDescriptor getShuffleDesc() {
        return shuffleDesc;
    }

    private void initShuffleWriteClients(
            int bufferSize, String dataPartitionFactoryName, ConnectionManager connectionManager) {
        JobID jobID = shuffleDesc.getJobId();
        DataSetID dataSetID = shuffleDesc.getDataSetId();
        MapPartitionID mapID = (MapPartitionID) shuffleDesc.getDataPartitionID();
        ShuffleWorkerDescriptor[] workerDescriptors = getShuffleWorkerDescriptors();
        long consumerGroupID = shuffleDesc.getShuffleResource().getConsumerGroupID();
        checkState(
                isMapPartition || numSubs == workerDescriptors.length,
                "Wrong number of workers, " + workerDescriptors.length + " " + numSubs);
        for (int i = 0; i < workerDescriptors.length; i++) {
            InetSocketAddress address =
                    new InetSocketAddress(
                            workerDescriptors[i].getWorkerAddress(),
                            workerDescriptors[i].getDataPort());
            if (shuffleWriteClients.containsKey(i)) {
                continue;
            }
            ReducePartitionID reducePartitionID = new ReducePartitionID(i, consumerGroupID);
            shuffleWriteClients.put(
                    i,
                    new ShuffleWriteClient(
                            address,
                            jobID,
                            dataSetID,
                            mapID,
                            reducePartitionID,
                            numMapsInGroup,
                            numSubs,
                            bufferSize,
                            isMapPartition,
                            dataPartitionFactoryName,
                            connectionManager,
                            writingClients::add));
        }
        checkState(
                shuffleWriteClients.size() == workerDescriptors.length, "Wrong write client count");
        initBufferPackers();
    }

    private ShuffleWorkerDescriptor[] getShuffleWorkerDescriptors() {
        return isMapPartition
                ? new ShuffleWorkerDescriptor[] {
                    shuffleDesc.getShuffleResource().getMapPartitionLocation()
                }
                : shuffleDesc.getShuffleResource().getReducePartitionLocations();
    }

    private void initBufferPackers() {
        shuffleWriteClients.forEach(
                (subPartitionIndex, shuffleWriteClient) -> {
                    bufferPackers.put(
                            subPartitionIndex, new BufferPacker(shuffleWriteClient::write));
                });
    }
}
