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

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataPartitionID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;
import com.alibaba.flink.shuffle.core.utils.PartitionUtils;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

import com.alibaba.metrics.Counter;
import com.alibaba.metrics.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * Harness used to write data to storage. It performs shuffle write by {@link PartitionedDataStore}.
 * The lifecycle is the same with a Netty {@link ChannelInboundHandler} instance.
 */
public class WritingService {

    private static final Logger LOG = LoggerFactory.getLogger(WritingService.class);

    private final PartitionedDataStore dataStore;

    private final Map<ChannelID, DataViewWriter> servingChannels = new HashMap<>();

    private final Map<ChannelID, ChannelHandlerContext> channelContexts = new HashMap<>();

    private final Map<ChannelID, Integer> channelRegionIndexes = new HashMap<>();

    private final Meter writingThroughputBytes;

    private final Counter numWritingFlows;

    public WritingService(PartitionedDataStore dataStore) {
        this.dataStore = dataStore;
        this.writingThroughputBytes = NetworkMetricsUtil.registerWritingThroughputBytes();
        this.numWritingFlows = NetworkMetricsUtil.registerNumWritingFlows();
    }

    public void handshake(
            ChannelID channelID,
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapID,
            ReducePartitionID reduceID,
            int numMaps,
            int numSubs,
            String dataPartitionFactory,
            BiConsumer<Integer, Integer> creditListener,
            Consumer<Throwable> failureListener,
            String addressStr)
            throws Throwable {

        checkState(
                !servingChannels.containsKey(channelID),
                () -> "Duplicate handshake for channel: " + channelID);
        long startTime = System.nanoTime();
        DataPartitionFactory factory = dataStore.partitionFactories().get(dataPartitionFactory);
        if (factory == null) {
            throw new ShuffleException(
                    "Can not find target partition factory in classpath or partition factory "
                            + "initialization failed.");
        }
        DataPartitionID dataPartitionID =
                PartitionUtils.isMapPartition(factory.getDataPartitionType()) ? mapID : reduceID;
        DataPartitionWritingView writingView =
                dataStore.createDataPartitionWritingView(
                        new WritingViewContext(
                                jobID,
                                dataSetID,
                                dataPartitionID,
                                mapID,
                                numMaps,
                                numSubs,
                                dataPartitionFactory,
                                creditListener::accept,
                                failureListener::accept));
        LOG.debug(
                "(channel: {}) Writing handshake cost {} ms.",
                channelID,
                (System.nanoTime() - startTime) / 1000_000);
        servingChannels.put(channelID, new DataViewWriter(writingView, addressStr));
        numWritingFlows.inc();
    }

    public void write(ChannelID channelID, int subIdx, ByteBuf byteBuf) {
        DataViewWriter dataViewWriter = servingChannels.get(channelID);
        if (dataViewWriter == null) {
            byteBuf.release();
            throw new IllegalStateException("Writing channel has been released -- " + channelID);
        }
        ReducePartitionID reduceID = new ReducePartitionID(subIdx);
        writingThroughputBytes.mark(byteBuf.readableBytes());
        checkState(channelRegionIndexes.containsKey(channelID), "Wrong channel indexes state");
        int regionIndex = channelRegionIndexes.get(channelID);
        dataViewWriter.getWritingView().onBuffer((Buffer) byteBuf, regionIndex, reduceID);
    }

    public Supplier<ByteBuf> getBufferSupplier(ChannelID channelID) {
        return () -> {
            checkState(servingChannels.containsKey(channelID), "Channel is not under serving.");
            return servingChannels.get(channelID).getWritingView().getBufferSupplier().pollBuffer();
        };
    }

    public void regionStart(
            ChannelID channelID,
            int regionIdx,
            int numMaps,
            int requireCredit,
            boolean isBroadcast) {
        DataViewWriter dataViewWriter = servingChannels.get(channelID);
        channelRegionIndexes.put(channelID, regionIdx);
        checkState(
                dataViewWriter != null,
                () -> String.format("Write-channel %s is not under serving.", channelID));
        dataViewWriter
                .getWritingView()
                .regionStarted(regionIdx, numMaps, requireCredit, isBroadcast);
    }

    public void regionFinish(ChannelID channelID, int regionIdx) {
        DataViewWriter dataViewWriter = servingChannels.get(channelID);
        checkState(
                dataViewWriter != null,
                () -> String.format("Write-channel %s is not under serving.", channelID));
        dataViewWriter.getWritingView().regionFinished(regionIdx);
    }

    public void writeFinish(ChannelID channelID, Runnable committedListener) {
        DataViewWriter dataViewWriter = servingChannels.get(channelID);
        checkState(
                dataViewWriter != null,
                () -> String.format("Write-channel %s is not under serving.", channelID));
        dataViewWriter.getWritingView().finish(committedListener::run);
        servingChannels.remove(channelID);
        numWritingFlows.dec();
    }

    private void clearServingChannels(ChannelID channelID) {
        servingChannels.remove(channelID);
        channelContexts.remove(channelID);
        channelRegionIndexes.remove(channelID);
    }

    public int getNumServingChannels() {
        return servingChannels.size();
    }

    Map<ChannelID, ChannelHandlerContext> getChannelContexts() {
        return channelContexts;
    }

    public void closeAbnormallyIfUnderServing(ChannelID channelID) {
        DataViewWriter dataViewWriter = servingChannels.get(channelID);
        if (dataViewWriter != null) {
            dataViewWriter
                    .getWritingView()
                    .onError(
                            new Exception(
                                    String.format(
                                            "(channel: %s) Channel closed abnormally.",
                                            channelID)));
            numWritingFlows.dec();
            clearServingChannels(channelID);
        }
    }

    public void releaseOnError(Throwable cause, ChannelID channelID) {
        if (channelID == null) {
            Set<ChannelID> channelIDs = servingChannels.keySet();
            LOG.error(
                    "Release channels -- {} on error.",
                    channelIDs.stream().map(ChannelID::toString).collect(Collectors.joining(", ")),
                    cause);
            for (DataViewWriter dataViewWriter : servingChannels.values()) {
                CommonUtils.runQuietly(() -> dataViewWriter.getWritingView().onError(cause), true);
            }
            numWritingFlows.dec(getNumServingChannels());
            servingChannels.clear();
        } else if (servingChannels.containsKey(channelID)) {
            LOG.error("Release channel -- {} on error. ", channelID, cause);
            CommonUtils.runQuietly(
                    () -> servingChannels.get(channelID).getWritingView().onError(cause), true);
            numWritingFlows.dec();
            clearServingChannels(channelID);
        }
    }
}
