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

package com.alibaba.flink.shuffle.core.storage;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/** Tests for {@link BufferQueue}. */
public class BufferQueueTest {

    @Test
    public void testPollBuffer() {
        ByteBuffer buffer1 = ByteBuffer.allocate(1024);
        ByteBuffer buffer2 = ByteBuffer.allocate(1024);
        BufferQueue bufferQueue = new BufferQueue(Arrays.asList(buffer1, buffer2));

        assertEquals(2, bufferQueue.size());
        assertEquals(buffer1, bufferQueue.poll());

        assertEquals(1, bufferQueue.size());
        assertEquals(buffer2, bufferQueue.poll());

        assertEquals(0, bufferQueue.size());
        assertNull(bufferQueue.poll());
    }

    @Test
    public void testAddBuffer() {
        ByteBuffer buffer1 = ByteBuffer.allocate(1024);
        ByteBuffer buffer2 = ByteBuffer.allocate(1024);
        BufferQueue bufferQueue = new BufferQueue(new ArrayList<>());

        assertEquals(0, bufferQueue.size());
        bufferQueue.add(buffer1);

        assertEquals(1, bufferQueue.size());
        bufferQueue.add(buffer2);

        assertEquals(2, bufferQueue.size());
        bufferQueue.add(Arrays.asList(ByteBuffer.allocate(1024), ByteBuffer.allocate(1024)));
        assertEquals(4, bufferQueue.size());
    }

    @Test
    public void testReleaseBufferQueue() {
        ByteBuffer buffer1 = ByteBuffer.allocate(1024);
        ByteBuffer buffer2 = ByteBuffer.allocate(1024);
        BufferQueue bufferQueue = new BufferQueue(Arrays.asList(buffer1, buffer2));

        assertEquals(2, bufferQueue.size());
        List<ByteBuffer> buffers = bufferQueue.release();
        assertEquals(buffer1, buffers.get(0));
        assertEquals(buffer2, buffers.get(1));

        assertEquals(0, bufferQueue.size());
        try {
            bufferQueue.add(buffer1);
        } catch (IllegalStateException exception) {
            assertNull(bufferQueue.poll());
            return;
        }

        fail("IllegalStateException expected.");
    }
}
