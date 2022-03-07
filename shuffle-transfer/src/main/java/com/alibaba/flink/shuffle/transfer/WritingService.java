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

package com.alibaba.flink.shuffle.transfer;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.ids.ChannelID;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWritingView;
import com.alibaba.flink.shuffle.core.storage.PartitionedDataStore;
import com.alibaba.flink.shuffle.core.storage.WritingViewContext;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandler;

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

    private final Map<ChannelID, DataViewWriter> servingChannels;

    private final Map<ChannelID, ChannelHandlerContext> channelContexts;

    private final Map<ChannelID, Integer> channelRegionIndexes;

    private int maxReducePartitionIndex = 0;

    public WritingService(PartitionedDataStore dataStore) {
        this.dataStore = dataStore;
        this.servingChannels = new HashMap<>();
        this.channelContexts = new HashMap<>();
        this.channelRegionIndexes = new HashMap<>();
    }

    public void handshake(
            ChannelID channelID,
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapID,
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
        DataPartitionWritingView writingView =
                dataStore.createDataPartitionWritingView(
                        new WritingViewContext(
                                jobID,
                                dataSetID,
                                mapID,
                                mapID,
                                numSubs,
                                dataPartitionFactory,
                                creditListener::accept,
                                failureListener::accept));
        LOG.debug(
                "(channel: {}) Writing handshake cost {} ms.",
                channelID,
                (System.nanoTime() - startTime) / 1000_000);
        servingChannels.put(channelID, new DataViewWriter(writingView, addressStr));
        NetworkMetrics.numWritingFlows().inc();
    }

    public void reducePartitionHandshake(
            ChannelID channelID,
            JobID jobID,
            DataSetID dataSetID,
            MapPartitionID mapID,
            ReducePartitionID reduceID,
            int numMapPartitions,
            int startSubIdx,
            int endSubIdx,
            String dataPartitionFactory,
            BiConsumer<Integer, Integer> creditListener,
            Consumer<Throwable> failureListener,
            ChannelHandlerContext ctx,
            String addressStr)
            throws Throwable {

        checkState(
                !servingChannels.containsKey(channelID),
                () -> "Duplicate handshake for channel: " + channelID);
        long startTime = System.nanoTime();
        channelContexts.put(channelID, ctx);
        DataPartitionWritingView writingView =
                dataStore.createDataPartitionWritingView(
                        new WritingViewContext(
                                jobID,
                                dataSetID,
                                reduceID,
                                mapID,
                                numMapPartitions,
                                startSubIdx,
                                endSubIdx,
                                dataPartitionFactory,
                                creditListener::accept,
                                failureListener::accept));

        maxReducePartitionIndex = Math.max(reduceID.getPartitionIndex(), maxReducePartitionIndex);
        LOG.debug(
                "(channel: {}) Writing handshake for reduce partition cost {} ms.",
                channelID,
                (System.nanoTime() - startTime) / 1000_000);
        servingChannels.put(channelID, new DataViewWriter(writingView, addressStr));
        NetworkMetrics.numWritingFlows().inc();
    }

    public void write(ChannelID channelID, int subIdx, ByteBuf byteBuf) {
        DataViewWriter dataViewWriter = servingChannels.get(channelID);
        if (dataViewWriter == null) {
            byteBuf.release();
            throw new IllegalStateException("Writing channel has been released -- " + channelID);
        }
        ReducePartitionID reduceID = new ReducePartitionID(subIdx);
        NetworkMetrics.numBytesWritingThroughput().mark(byteBuf.readableBytes());
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
        NetworkMetrics.numWritingFlows().dec();
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
            clearServingChannels(channelID);
            NetworkMetrics.numWritingFlows().dec();
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
            NetworkMetrics.numWritingFlows().dec(getNumServingChannels());
            servingChannels.clear();
        } else if (servingChannels.containsKey(channelID)) {
            LOG.error("Release channel -- {} on error. ", channelID, cause);
            CommonUtils.runQuietly(
                    () -> servingChannels.get(channelID).getWritingView().onError(cause), true);
            clearServingChannels(channelID);
            NetworkMetrics.numWritingFlows().dec();
        }
    }
}
