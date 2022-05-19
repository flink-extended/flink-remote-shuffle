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
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.utils.BufferUtils;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ShuffleReadClient;
import com.alibaba.flink.shuffle.transfer.TransferBufferPool;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.SubpartitionIndexRange;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** A {@link IndexedInputGate} which ingest data from remote shuffle workers. */
public class RemoteShuffleInputGate extends IndexedInputGate {

    private static final Logger LOG = LoggerFactory.getLogger(RemoteShuffleInputGate.class);

    /** Lock to protect {@link #receivedBuffers} and {@link #cause} and {@link #closed}. */
    private final Object lock = new Object();

    /** Name of the corresponding computing task. */
    private final String taskName;

    /** Used to manage physical connections. */
    private final ConnectionManager connectionManager;

    /** Index of the gate of the corresponding computing task. */
    private final int gateIndex;

    /** Deployment descriptor for a single input gate instance. */
    private final InputGateDeploymentDescriptor gateDescriptor;

    /** Number of concurrent readings. */
    private final int numConcurrentReading;

    /** Buffer pool provider. */
    private final SupplierWithException<BufferPool, IOException> bufferPoolFactory;

    /** Flink buffer pools to allocate network memory. */
    private BufferPool bufferPool;

    /** Buffer pool used by the transfer layer. */
    private final TransferBufferPool transferBufferPool =
            new TransferBufferPool(Collections.emptySet());

    /** Data decompressor. */
    private final BufferDecompressor bufferDecompressor;

    /** {@link InputChannelInfo}s to describe channels. */
    private final List<InputChannelInfo> channelsInfo;

    /** A {@link ShuffleReadClient} corresponds to a reading channel. */
    private final List<ShuffleReadClient> shuffleReadClients = new ArrayList<>();

    /** Map from channel index to shuffle client index. */
    private final int[] clientIndexMap;

    /** Map from shuffle client index to channel index. */
    private final int[] channelIndexMap;

    /** The number of subpartitions that has not consumed per channel. */
    private final int[] numSubPartitionsHasNotConsumed;

    /** The overall number of subpartitions that has not been consumed. */
    private long numUnconsumedSubpartitions;

    /** Received buffers from remote shuffle worker. It's consumed by upper computing task. */
    @GuardedBy("lock")
    private final Queue<Pair<Buffer, InputChannelInfo>> receivedBuffers = new LinkedList<>();

    /** {@link Throwable} when reading failure. */
    @GuardedBy("lock")
    private Throwable cause;

    /** Whether this remote input gate has been closed or not. */
    @GuardedBy("lock")
    private boolean closed;

    /** Whether we have opened all initial channels or not. */
    private boolean initialChannelsOpened;

    /** Number of pending {@link EndOfData} events to be received. */
    private long pendingEndOfDataEvents;

    /** Keep compatibility with streaming mode. */
    private boolean shouldDrainOnEndOfData = true;

    public RemoteShuffleInputGate(
            String taskName,
            boolean shuffleChannels,
            int gateIndex,
            int networkBufferSize,
            InputGateDeploymentDescriptor gateDescriptor,
            int numConcurrentReading,
            ConnectionManager connectionManager,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            BufferDecompressor bufferDecompressor) {

        this.taskName = taskName;
        this.gateIndex = gateIndex;
        this.gateDescriptor = gateDescriptor;
        this.numConcurrentReading = numConcurrentReading;
        this.connectionManager = connectionManager;
        this.bufferPoolFactory = bufferPoolFactory;
        this.bufferDecompressor = bufferDecompressor;

        int numChannels = gateDescriptor.getShuffleDescriptors().length;
        this.clientIndexMap = new int[numChannels];
        this.channelIndexMap = new int[numChannels];
        this.numSubPartitionsHasNotConsumed = new int[numChannels];
        this.channelsInfo = createChannelInfos();
        this.numUnconsumedSubpartitions =
                initShuffleReadClients(networkBufferSize, shuffleChannels);
        this.pendingEndOfDataEvents = numUnconsumedSubpartitions;
    }

    private long initShuffleReadClients(int bufferSize, boolean shuffleChannels) {
        int startSubIdx = gateDescriptor.getConsumedSubpartitionIndexRange().getStartIndex();
        int endSubIdx = gateDescriptor.getConsumedSubpartitionIndexRange().getEndIndex();
        checkState(endSubIdx >= startSubIdx);
        int numSubpartitionsPerChannel = endSubIdx - startSubIdx + 1;
        long numUnconsumedSubpartitions = 0;

        List<Pair<Integer, ShuffleDescriptor>> descriptors =
                IntStream.range(0, gateDescriptor.getShuffleDescriptors().length)
                        .mapToObj(i -> Pair.of(i, gateDescriptor.getShuffleDescriptors()[i]))
                        .collect(Collectors.toList());
        if (shuffleChannels) {
            Collections.shuffle(descriptors);
        }

        int clientIndex = 0;
        for (Pair<Integer, ShuffleDescriptor> descriptor : descriptors) {
            RemoteShuffleDescriptor remoteDescriptor =
                    (RemoteShuffleDescriptor) descriptor.getRight();
            ShuffleWorkerDescriptor swd =
                    remoteDescriptor.getShuffleResource().getMapPartitionLocation();
            InetSocketAddress address =
                    new InetSocketAddress(swd.getWorkerAddress(), swd.getDataPort());
            LOG.debug(
                    "Create DataPartitionReader [dataSetID: {}, resultPartitionID: {}, channelIdx: "
                            + "{}, mapID: {}, startSubIdx: {}, endSubIdx {}, address: {}]",
                    remoteDescriptor.getDataSetId(),
                    remoteDescriptor.getResultPartitionID(),
                    descriptor.getLeft(),
                    remoteDescriptor.getDataPartitionID(),
                    startSubIdx,
                    endSubIdx,
                    address);
            ShuffleReadClient shuffleReadClient =
                    createShuffleReadClient(
                            connectionManager,
                            address,
                            remoteDescriptor.getDataSetId(),
                            (MapPartitionID) remoteDescriptor.getDataPartitionID(),
                            startSubIdx,
                            endSubIdx,
                            bufferSize,
                            transferBufferPool,
                            getDataListener(descriptor.getLeft()),
                            getFailureListener(remoteDescriptor.getResultPartitionID()));

            shuffleReadClients.add(shuffleReadClient);
            numSubPartitionsHasNotConsumed[descriptor.getLeft()] = numSubpartitionsPerChannel;
            numUnconsumedSubpartitions += numSubpartitionsPerChannel;
            clientIndexMap[descriptor.getLeft()] = clientIndex;
            channelIndexMap[clientIndex] = descriptor.getLeft();
            ++clientIndex;
        }
        return numUnconsumedSubpartitions;
    }

    /** Setup gate and build network connections. */
    @Override
    public void setup() throws IOException {
        long startTime = System.nanoTime();

        bufferPool = bufferPoolFactory.get();
        BufferUtils.reserveNumRequiredBuffers(
                bufferPool, RemoteShuffleInputGateFactory.MIN_BUFFERS_PER_GATE);

        try {
            for (int i = 0; i < gateDescriptor.getShuffleDescriptors().length; i++) {
                shuffleReadClients.get(i).connect();
            }
        } catch (Throwable throwable) {
            LOG.error("Failed to setup remote input gate.", throwable);
            ExceptionUtils.rethrowAsRuntimeException(throwable);
        }

        tryRequestBuffers();
        // Complete availability future though handshake not fired yet, thus to allow fetcher to
        // 'pollNext' and fire handshake to remote. This mechanism is to avoid bookkeeping remote
        // reading resource before task start processing data from input gate.
        availabilityHelper.getUnavailableToResetAvailable().complete(null);
        LOG.info("Set up read gate by {} ms.", (System.nanoTime() - startTime) / 1000_000);
    }

    /** Index of the gate of the corresponding computing task. */
    @Override
    public int getGateIndex() {
        return gateIndex;
    }

    /** Get number of input channels. A channel is a data flow from one shuffle worker. */
    @Override
    public int getNumberOfInputChannels() {
        return channelsInfo.size();
    }

    /** Whether reading is finished -- all channels are finished and cached buffers are drained. */
    @Override
    public boolean isFinished() {
        synchronized (lock) {
            return allReadersEOF() && receivedBuffers.isEmpty();
        }
    }

    @Override
    public Optional<BufferOrEvent> getNext() {
        throw new UnsupportedOperationException("Not implemented (DataSet API is not supported).");
    }

    /** Poll a received {@link BufferOrEvent}. */
    @Override
    public Optional<BufferOrEvent> pollNext() throws IOException {
        if (!initialChannelsOpened) {
            tryOpenSomeChannels();
            initialChannelsOpened = true;
            // DO NOT return, method of 'getReceived' will manipulate 'availabilityHelper'.
        }

        Pair<Buffer, InputChannelInfo> pair = getReceived();
        Optional<BufferOrEvent> bufferOrEvent = Optional.empty();
        while (pair != null) {
            Buffer buffer = pair.getLeft();
            InputChannelInfo channelInfo = pair.getRight();

            if (buffer.isBuffer()) {
                bufferOrEvent = transformBuffer(buffer, channelInfo);
            } else {
                bufferOrEvent = transformEvent(buffer, channelInfo);
            }

            if (bufferOrEvent.isPresent()) {
                break;
            }
            pair = getReceived();
        }

        tryRequestBuffers();
        return bufferOrEvent;
    }

    /** Close all reading channels inside this {@link RemoteShuffleInputGate}. */
    @Override
    public void close() throws Exception {
        List<Buffer> buffersToRecycle;
        Throwable closeException = null;
        synchronized (lock) {
            // Do not check closed flag, thus to allow calling this method from both task thread and
            // cancel thread.
            for (ShuffleReadClient shuffleReadClient : shuffleReadClients) {
                try {
                    shuffleReadClient.close();
                } catch (Throwable throwable) {
                    closeException = closeException == null ? throwable : closeException;
                    LOG.error("Failed to close shuffle read client.", throwable);
                }
            }
            buffersToRecycle =
                    receivedBuffers.stream().map(Pair::getLeft).collect(Collectors.toList());
            receivedBuffers.clear();
            closed = true;
        }

        try {
            buffersToRecycle.forEach(Buffer::recycleBuffer);
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to recycle buffers.", throwable);
        }

        try {
            transferBufferPool.destroy();
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close transfer buffer pool.", throwable);
        }

        try {
            if (bufferPool != null) {
                bufferPool.lazyDestroy();
            }
        } catch (Throwable throwable) {
            closeException = closeException == null ? throwable : closeException;
            LOG.error("Failed to close local buffer pool.", throwable);
        }

        if (closeException != null) {
            ExceptionUtils.rethrowException(closeException);
        }
    }

    /** Get {@link InputChannelInfo}s of this {@link RemoteShuffleInputGate}. */
    @Override
    public List<InputChannelInfo> getChannelInfos() {
        return channelsInfo;
    }

    /** Get all {@link ShuffleReadClient}s inside. Each one corresponds to a reading channel. */
    public List<ShuffleReadClient> getShuffleReadClients() {
        return shuffleReadClients;
    }

    private List<InputChannelInfo> createChannelInfos() {
        return IntStream.range(0, gateDescriptor.getShuffleDescriptors().length)
                .mapToObj(i -> new InputChannelInfo(gateIndex, i))
                .collect(Collectors.toList());
    }

    ShuffleReadClient createShuffleReadClient(
            ConnectionManager connectionManager,
            InetSocketAddress address,
            DataSetID dataSetID,
            MapPartitionID mapID,
            int startSubIdx,
            int endSubIdx,
            int bufferSize,
            TransferBufferPool bufferPool,
            Consumer<ByteBuf> dataListener,
            Consumer<Throwable> failureListener) {
        return new ShuffleReadClient(
                address,
                dataSetID,
                mapID,
                startSubIdx,
                endSubIdx,
                bufferSize,
                bufferPool,
                connectionManager,
                dataListener,
                failureListener);
    }

    /** Try to open more readers to {@link #numConcurrentReading}. */
    private void tryOpenSomeChannels() throws IOException {
        List<ShuffleReadClient> clientsToOpen = new ArrayList<>();
        synchronized (lock) {
            if (closed) {
                throw new IOException("Input gate already closed.");
            }

            LOG.debug("Try open some partition readers.");
            int numOnGoing = 0;
            for (int i = 0; i < shuffleReadClients.size(); i++) {
                ShuffleReadClient shuffleReadClient = shuffleReadClients.get(i);
                LOG.debug(
                        "Trying reader: {}, isOpened={}, numSubPartitionsHasNotConsumed={}.",
                        shuffleReadClient,
                        shuffleReadClient.isOpened(),
                        numSubPartitionsHasNotConsumed[channelIndexMap[i]]);
                if (numOnGoing >= numConcurrentReading) {
                    break;
                }

                if (shuffleReadClient.isOpened()
                        && numSubPartitionsHasNotConsumed[channelIndexMap[i]] > 0) {
                    numOnGoing++;
                    continue;
                }

                if (!shuffleReadClient.isOpened()) {
                    clientsToOpen.add(shuffleReadClient);
                    numOnGoing++;
                }
            }
        }

        for (ShuffleReadClient shuffleReadClient : clientsToOpen) {
            shuffleReadClient.open();
        }
    }

    private void tryRequestBuffers() {
        checkState(bufferPool != null, "Not initialized yet.");

        Buffer buffer;
        List<ByteBuf> buffers = new ArrayList<>();
        while ((buffer = bufferPool.requestBuffer()) != null) {
            buffers.add(buffer.asByteBuf());
        }

        if (!buffers.isEmpty()) {
            transferBufferPool.addBuffers(buffers);
        }
    }

    private void onBuffer(Buffer buffer, int channelIdx) {
        synchronized (lock) {
            if (closed || cause != null) {
                buffer.recycleBuffer();
                throw new IllegalStateException("Input gate already closed or failed.");
            }

            boolean needRecycle = true;
            try {
                boolean wasEmpty = receivedBuffers.isEmpty();
                InputChannelInfo channelInfo = channelsInfo.get(channelIdx);
                checkState(
                        channelInfo.getInputChannelIdx() == channelIdx, "Illegal channel index.");
                receivedBuffers.add(Pair.of(buffer, channelInfo));
                needRecycle = false;
                if (wasEmpty) {
                    availabilityHelper.getUnavailableToResetAvailable().complete(null);
                }
            } catch (Throwable throwable) {
                if (needRecycle) {
                    buffer.recycleBuffer();
                }
                throw throwable;
            }
        }
    }

    private Consumer<ByteBuf> getDataListener(int channelIdx) {
        return byteBuf -> {
            Queue<Buffer> unpackedBuffers = null;
            try {
                unpackedBuffers = BufferPacker.unpack(byteBuf);
                while (!unpackedBuffers.isEmpty()) {
                    onBuffer(unpackedBuffers.poll(), channelIdx);
                }
            } catch (Throwable throwable) {
                synchronized (lock) {
                    cause = cause == null ? throwable : cause;
                    availabilityHelper.getUnavailableToResetAvailable().complete(null);
                }

                if (unpackedBuffers != null) {
                    unpackedBuffers.forEach(Buffer::recycleBuffer);
                }
                LOG.error("Failed to process the received buffer.", throwable);
            }
        };
    }

    private Consumer<Throwable> getFailureListener(ResultPartitionID rpID) {
        return throwable -> {
            synchronized (lock) {
                if (cause != null) {
                    return;
                }
                Class<?> clazz =
                        com.alibaba.flink.shuffle.core.exception.PartitionNotFoundException.class;
                if (throwable.getMessage() != null
                        && throwable.getMessage().contains(clazz.getName())) {
                    cause = new PartitionNotFoundException(rpID, throwable.getMessage());
                } else {
                    cause = throwable;
                }
                availabilityHelper.getUnavailableToResetAvailable().complete(null);
            }
        };
    }

    private Pair<Buffer, InputChannelInfo> getReceived() throws IOException {
        synchronized (lock) {
            healthCheck();
            if (!receivedBuffers.isEmpty()) {
                return receivedBuffers.poll();
            } else {
                if (!allReadersEOF()) {
                    availabilityHelper.resetUnavailable();
                }
                return null;
            }
        }
    }

    private void healthCheck() throws IOException {
        if (closed) {
            throw new IOException("Input gate already closed.");
        }
        if (cause != null) {
            if (cause instanceof IOException) {
                throw (IOException) cause;
            } else {
                throw new IOException(cause);
            }
        }
    }

    private boolean allReadersEOF() {
        return numUnconsumedSubpartitions <= 0;
    }

    private Optional<BufferOrEvent> transformBuffer(Buffer buf, InputChannelInfo info)
            throws IOException {
        return Optional.of(
                new BufferOrEvent(decompressBufferIfNeeded(buf), info, !isFinished(), false));
    }

    private Optional<BufferOrEvent> transformEvent(Buffer buffer, InputChannelInfo channelInfo)
            throws IOException {
        final AbstractEvent event;
        try {
            event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        } catch (Throwable t) {
            throw new IOException("Deserialize failure.", t);
        } finally {
            buffer.recycleBuffer();
        }

        if (event.getClass() == EndOfPartitionEvent.class) {
            checkState(
                    numSubPartitionsHasNotConsumed[channelInfo.getInputChannelIdx()] > 0,
                    "BUG -- EndOfPartitionEvent received repeatedly.");
            numSubPartitionsHasNotConsumed[channelInfo.getInputChannelIdx()]--;
            numUnconsumedSubpartitions--;
            // not the real end.
            if (numSubPartitionsHasNotConsumed[channelInfo.getInputChannelIdx()] != 0) {
                return Optional.empty();
            } else {
                // the real end.
                shuffleReadClients.get(clientIndexMap[channelInfo.getInputChannelIdx()]).close();
                tryOpenSomeChannels();
                if (allReadersEOF()) {
                    availabilityHelper.getUnavailableToResetAvailable().complete(null);
                }
            }
        } else if (event.getClass() == EndOfData.class) {
            CommonUtils.checkState(pendingEndOfDataEvents > 0, "Too many EndOfData event.");
            --pendingEndOfDataEvents;
            shouldDrainOnEndOfData &= ((EndOfData) event).getStopMode() == StopMode.DRAIN;
        }

        return Optional.of(
                new BufferOrEvent(
                        event,
                        buffer.getDataType().hasPriority(),
                        channelInfo,
                        !isFinished(),
                        buffer.getSize(),
                        false));
    }

    private Buffer decompressBufferIfNeeded(Buffer buffer) throws IOException {
        if (buffer.isCompressed()) {
            try {
                checkState(bufferDecompressor != null, "Buffer decompressor not set.");
                return bufferDecompressor.decompressToIntermediateBuffer(buffer);
            } catch (Throwable t) {
                throw new IOException("Decompress failure", t);
            } finally {
                buffer.recycleBuffer();
            }
        }
        return buffer;
    }

    @Override
    public void requestPartitions() {
        // do-nothing
    }

    @Override
    public void checkpointStarted(CheckpointBarrier barrier) {
        // do-nothing.
    }

    @Override
    public void checkpointStopped(long cancelledCheckpointId) {
        // do-nothing.
    }

    @Override
    public List<InputChannelInfo> getUnfinishedChannels() {
        return Collections.emptyList();
    }

    @Override
    public void triggerDebloating() {
        // do-nothing.
    }

    @Override
    public EndOfDataStatus hasReceivedEndOfData() {
        if (pendingEndOfDataEvents > 0) {
            return EndOfDataStatus.NOT_END_OF_DATA;
        } else if (shouldDrainOnEndOfData) {
            return EndOfDataStatus.DRAINED;
        } else {
            return EndOfDataStatus.STOPPED;
        }
    }

    @Override
    public void finishReadRecoveredState() {
        // do-nothing.
    }

    @Override
    public InputChannel getChannel(int channelIndex) {
        return new FakedRemoteInputChannel(channelIndex);
    }

    @Override
    public void sendTaskEvent(TaskEvent event) {
        throw new FlinkRuntimeException("Method should not be called.");
    }

    @Override
    public void resumeConsumption(InputChannelInfo channelInfo) {
        throw new FlinkRuntimeException("Method should not be called.");
    }

    @Override
    public void acknowledgeAllRecordsProcessed(InputChannelInfo inputChannelInfo) {}

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String toString() {
        return String.format(
                "ReadGate [owning task: %s, gate index: %d, descriptor: %s]",
                taskName, gateIndex, gateDescriptor.toString());
    }

    /** Accommodation for the incompleteness of Flink pluggable shuffle service. */
    private class FakedRemoteInputChannel extends RemoteInputChannel {
        FakedRemoteInputChannel(int channelIndex) {
            super(
                    new SingleInputGate(
                            "",
                            gateIndex,
                            new IntermediateDataSetID(),
                            ResultPartitionType.BLOCKING,
                            new SubpartitionIndexRange(0, 0),
                            1,
                            (a, b, c) -> {},
                            () -> null,
                            null,
                            new FakedMemorySegmentProvider(),
                            0,
                            new ThroughputCalculator(SystemClock.getInstance()),
                            null),
                    channelIndex,
                    new ResultPartitionID(),
                    0,
                    new ConnectionID(new InetSocketAddress("", 0), 0),
                    new LocalConnectionManager(),
                    0,
                    0,
                    0,
                    new SimpleCounter(),
                    new SimpleCounter(),
                    new FakedChannelStateWriter());
        }
    }

    /** Accommodation for the incompleteness of Flink pluggable shuffle service. */
    private static class FakedMemorySegmentProvider implements MemorySegmentProvider {
        @Override
        public Collection<MemorySegment> requestUnpooledMemorySegments(int i) {
            return null;
        }

        @Override
        public void recycleUnpooledMemorySegments(Collection<MemorySegment> collection) {}
    }

    /** Accommodation for the incompleteness of Flink pluggable shuffle service. */
    private static class FakedChannelStateWriter implements ChannelStateWriter {

        @Override
        public void start(long cpId, CheckpointOptions checkpointOptions) {}

        @Override
        public void addInputData(
                long cpId,
                InputChannelInfo info,
                int startSeqNum,
                CloseableIterator<Buffer> data) {}

        @Override
        public void addOutputData(
                long cpId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {}

        @Override
        public void finishInput(long checkpointId) {}

        @Override
        public void finishOutput(long checkpointId) {}

        @Override
        public void abort(long checkpointId, Throwable cause, boolean cleanup) {}

        @Override
        public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
            return null;
        }

        @Override
        public void close() {}
    }
}
