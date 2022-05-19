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

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test is to assert Flink {@link
 * org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor} will not ingest from input
 * before really processing the data.
 */
public class StreamProcessorTest {

    @Test
    public void testMultipleInputProcessor() throws Exception {
        boolean[] eofs = new boolean[2];

        AtomicBoolean emitted0 = new AtomicBoolean(false);
        Deque<DataInputStatus> statuses0 =
                new ArrayDeque<>(
                        Arrays.asList(
                                DataInputStatus.MORE_AVAILABLE, DataInputStatus.END_OF_INPUT));
        StreamOneInputProcessor<?> processor0 = getOneInputProcessor(0, statuses0, emitted0, eofs);

        AtomicBoolean emitted1 = new AtomicBoolean(false);
        Deque<DataInputStatus> statuses1 =
                new ArrayDeque<>(Collections.singletonList(DataInputStatus.END_OF_INPUT));
        StreamOneInputProcessor<?> processor1 = getOneInputProcessor(1, statuses1, emitted1, eofs);

        InputSelectable inputSelectable = getInputSelectableForTwoInputProcessor(eofs);
        final StreamInputProcessor processor;
        processor = getMultipleInputProcessor(inputSelectable, processor0, processor1);

        assertFalse(emitted0.get());
        assertFalse(emitted1.get());
        assertFalse(eofs[0]);

        processor.processInput();
        assertTrue(emitted0.get() && !emitted1.get());
        assertFalse(eofs[0]);

        processor.processInput();
        assertTrue(emitted0.get() && !emitted1.get());
        assertTrue(eofs[0]);

        processor.processInput();
        assertTrue(emitted0.get() && emitted1.get());
        assertTrue(eofs[1]);
    }

    private StreamMultipleInputProcessor getMultipleInputProcessor(
            InputSelectable inputSelectable, StreamOneInputProcessor<?>... inputProcessors) {
        return new StreamMultipleInputProcessor(
                new MultipleInputSelectionHandler(inputSelectable, inputProcessors.length),
                inputProcessors);
    }

    private InputSelectable getInputSelectableForTwoInputProcessor(boolean[] eofs) {
        return () -> {
            if (!eofs[0]) {
                return InputSelection.FIRST;
            } else {
                return InputSelection.SECOND;
            }
        };
    }

    private StreamOneInputProcessor<?> getOneInputProcessor(
            int inputIdx,
            Deque<DataInputStatus> inputStatuses,
            AtomicBoolean recordEmitted,
            boolean[] endOfInputs) {

        PushingAsyncDataInput.DataOutput<Object> output =
                new PushingAsyncDataInput.DataOutput<Object>() {
                    @Override
                    public void emitRecord(StreamRecord streamRecord) {}

                    @Override
                    public void emitWatermark(Watermark watermark) {}

                    @Override
                    public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

                    @Override
                    public void emitLatencyMarker(LatencyMarker latencyMarker) {}
                };

        StreamTaskInput<Object> taskInput =
                new StreamTaskInput<Object>() {
                    @Override
                    public int getInputIndex() {
                        return inputIdx;
                    }

                    @Override
                    public CompletableFuture<Void> prepareSnapshot(
                            ChannelStateWriter channelStateWriter, long l) {
                        return null;
                    }

                    @Override
                    public void close() {}

                    @Override
                    public DataInputStatus emitNext(DataOutput<Object> dataOutput) {
                        recordEmitted.set(true);
                        DataInputStatus inputStatus = inputStatuses.poll();
                        if (inputStatus == DataInputStatus.END_OF_INPUT) {
                            endOfInputs[inputIdx] = true;
                        }
                        return inputStatus;
                    }

                    @Override
                    public CompletableFuture<?> getAvailableFuture() {
                        return null;
                    }
                };
        return new StreamOneInputProcessor<>(taskInput, output, ignored -> {});
    }
}
