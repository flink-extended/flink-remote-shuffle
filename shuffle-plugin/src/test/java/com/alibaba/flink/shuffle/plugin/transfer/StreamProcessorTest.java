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

package com.alibaba.flink.shuffle.plugin.transfer;

import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.streaming.api.operators.BoundedMultiInput;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.MultipleInputSelectionHandler;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskInput;
import org.apache.flink.streaming.runtime.io.StreamTwoInputProcessor;
import org.apache.flink.streaming.runtime.io.TwoInputSelectionHandler;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;

import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * This test is to assert Flink {@link StreamTwoInputProcessor} and {@link
 * org.apache.flink.streaming.runtime.io.StreamMultipleInputProcessor} will not injest from input
 * before really processing the data.
 */
public class StreamProcessorTest {

    @Test
    public void testTwoInputProcessor() throws Exception {
        test(true);
    }

    @Test
    public void testMultipleInputProcessor() throws Exception {
        test(false);
    }

    private void test(boolean testTwoInputProcessor) throws Exception {
        boolean[] eofs = new boolean[2];

        AtomicBoolean emitted0 = new AtomicBoolean(false);
        Deque<InputStatus> statuses0 =
                new ArrayDeque<>(
                        Lists.newArrayList(InputStatus.MORE_AVAILABLE, InputStatus.END_OF_INPUT));
        StreamOneInputProcessor<Object> processor0 =
                getOneInputProcessor(0, statuses0, emitted0, eofs);

        AtomicBoolean emitted1 = new AtomicBoolean(false);
        Deque<InputStatus> statuses1 =
                new ArrayDeque<>(Lists.newArrayList(InputStatus.END_OF_INPUT));
        StreamOneInputProcessor<Object> processor1 =
                getOneInputProcessor(1, statuses1, emitted1, eofs);

        InputSelectable inputSelectable = getInputSelectableForTwoInputProcessor(eofs);
        final StreamInputProcessor processor;
        if (testTwoInputProcessor) {
            processor = getMultipleInputProcessor(inputSelectable, 2, processor0, processor1);
        } else {
            processor = getTwoInputProcessor(inputSelectable, processor0, processor1);
        }

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
            InputSelectable inputSelectable,
            int inputCount,
            StreamOneInputProcessor<Object>... inputProcessors) {
        return new StreamMultipleInputProcessor(
                new MultipleInputSelectionHandler(inputSelectable, inputCount), inputProcessors);
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

    private StreamTwoInputProcessor<Object, Object> getTwoInputProcessor(
            InputSelectable inputSelectable,
            StreamOneInputProcessor<Object> processor0,
            StreamOneInputProcessor<Object> processor1) {
        return new StreamTwoInputProcessor<>(
                new TwoInputSelectionHandler(inputSelectable), processor0, processor1);
    }

    private StreamOneInputProcessor<Object> getOneInputProcessor(
            int inputIdx,
            Deque<InputStatus> inputStatuses,
            AtomicBoolean recordEmitted,
            boolean[] endOfInputs) {

        PushingAsyncDataInput.DataOutput<Object> output =
                new PushingAsyncDataInput.DataOutput<Object>() {
                    @Override
                    public void emitRecord(StreamRecord streamRecord) {}

                    @Override
                    public void emitWatermark(Watermark watermark) {}

                    @Override
                    public void emitStreamStatus(StreamStatus streamStatus) {}

                    @Override
                    public void emitLatencyMarker(LatencyMarker latencyMarker) {}
                };

        BoundedMultiInput endOfInputAware = i -> endOfInputs[i - 1] = true;

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
                    public InputStatus emitNext(DataOutput<Object> dataOutput) {
                        recordEmitted.set(true);
                        return inputStatuses.poll();
                    }

                    @Override
                    public CompletableFuture<?> getAvailableFuture() {
                        return null;
                    }
                };
        return new StreamOneInputProcessor<>(taskInput, output, endOfInputAware);
    }
}
