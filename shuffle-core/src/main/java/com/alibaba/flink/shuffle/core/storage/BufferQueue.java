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

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;

/** A buffer queue implementation enhanced with a <b>release</b> state. */
@NotThreadSafe
public class BufferQueue {

    public static final BufferQueue RELEASED_EMPTY_BUFFER_QUEUE = new BufferQueue();

    /** All available buffers in this buffer queue. */
    private final Queue<ByteBuffer> buffers;

    /** Whether this buffer queue is released or not. */
    private boolean isReleased;

    public BufferQueue(List<ByteBuffer> buffers) {
        CommonUtils.checkArgument(buffers != null, "Must be not null.");
        this.buffers = new ArrayDeque<>(buffers);
    }

    private BufferQueue() {
        this.isReleased = true;
        this.buffers = new ArrayDeque<>();
    }

    /** Returns the number of available buffers in this buffer queue. */
    public int size() {
        return buffers.size();
    }

    /**
     * Returns an available buffer from this buffer queue or returns null if no buffer is available
     * currently.
     */
    @Nullable
    public ByteBuffer poll() {
        return buffers.poll();
    }

    /**
     * Adds an available buffer to this buffer queue and will throw exception if this buffer queue
     * has been released.
     */
    public void add(ByteBuffer availableBuffer) {
        CommonUtils.checkArgument(availableBuffer != null, "Must be not null.");
        CommonUtils.checkState(!isReleased, "Buffer queue has been released.");

        buffers.add(availableBuffer);
    }

    /**
     * Adds a collection of available buffers to this buffer queue and will throw exception if this
     * buffer queue has been released.
     */
    public void add(Collection<ByteBuffer> availableBuffers) {
        CommonUtils.checkArgument(availableBuffers != null, "Must be not null.");
        CommonUtils.checkState(!isReleased, "Buffer queue has been released.");

        buffers.addAll(availableBuffers);
    }

    /**
     * Releases this buffer queue and returns all available buffers. After released, no buffer can
     * be added to or polled from this buffer queue.
     */
    public List<ByteBuffer> release() {
        isReleased = true;

        List<ByteBuffer> released = new ArrayList<>(buffers);
        buffers.clear();
        return released;
    }

    /** Returns true is this buffer queue has been released. */
    public boolean isReleased() {
        return isReleased;
    }
}
