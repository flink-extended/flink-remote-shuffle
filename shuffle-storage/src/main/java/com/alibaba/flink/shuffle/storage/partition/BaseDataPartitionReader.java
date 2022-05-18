/*
 * Copyright 2021 Alibaba Group Holding Limited.
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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;
import com.alibaba.flink.shuffle.core.utils.ListenerUtils;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * {@link BaseDataPartitionReader} implements some basic logics of {@link DataPartitionReader} which
 * can be reused by subclasses and simplify the implementation of new {@link DataPartitionReader}s.
 */
public abstract class BaseDataPartitionReader implements DataPartitionReader {

    /**
     * Lock used for synchronization and to avoid potential race conditions between the data reading
     * thread and the data partition executor thread.
     */
    protected final Object lock = new Object();

    /** Listener to be notified when there is data available for consumption. */
    protected final DataListener dataListener;

    /** Listener to be notified when there is backlog available in this reader. */
    protected final BacklogListener backlogListener;

    /** Listener to be notified if any failure occurs while reading the data. */
    protected final FailureListener failureListener;

    /** All {@link Buffer}s read and can be polled from this partition reader. */
    @GuardedBy("lock")
    protected final Queue<Buffer> buffersRead = new ArrayDeque<>();

    /** Whether all the data has been successfully read or not. */
    @GuardedBy("lock")
    protected boolean isFinished;

    /** Whether this partition reader has been released or not. */
    @GuardedBy("lock")
    protected boolean isReleased;

    /** Exception causing the release of this partition reader. */
    @GuardedBy("lock")
    protected Throwable releaseCause;

    /** Whether there is any error at the consumer side or not. */
    @GuardedBy("lock")
    protected boolean isError;

    public BaseDataPartitionReader(
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        CommonUtils.checkArgument(dataListener != null, "Must be not null.");
        CommonUtils.checkArgument(backlogListener != null, "Must be not null.");
        CommonUtils.checkArgument(failureListener != null, "Must be not null.");

        this.dataListener = dataListener;
        this.backlogListener = backlogListener;
        this.failureListener = failureListener;
    }

    @Override
    public BufferWithBacklog nextBuffer() {
        synchronized (lock) {
            Buffer buffer = buffersRead.poll();
            if (buffer == null) {
                return null;
            }

            return new BufferWithBacklog(buffer, buffersRead.size());
        }
    }

    /**
     * Adds a buffer read to this {@link DataPartitionReader} for consumption and notifies the
     * target {@link DataListener} of the available data if needed.
     */
    protected void addBuffer(Buffer buffer, boolean hasRemaining) {
        if (buffer == null) {
            return;
        }

        final boolean recycleBuffer;
        boolean notifyDataAvailable = false;
        final Throwable throwable;

        synchronized (lock) {
            recycleBuffer = isReleased || isFinished || isError;
            throwable = releaseCause;
            isFinished = !hasRemaining;

            if (!recycleBuffer) {
                notifyDataAvailable = buffersRead.isEmpty();
                buffersRead.add(buffer);
            }
        }

        if (recycleBuffer) {
            BufferUtils.recycleBuffer(buffer);
            throw new ShuffleException("Partition reader has been failed or finished.", throwable);
        }

        if (notifyDataAvailable) {
            ListenerUtils.notifyAvailableData(dataListener);
        }
    }

    protected void notifyBacklog(int backlog) {
        ListenerUtils.notifyBacklog(backlogListener, backlog);
    }

    @Override
    public void release(Throwable throwable) throws Exception {
        boolean notifyFailure;
        Queue<Buffer> buffers;

        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            releaseCause = throwable;

            notifyFailure = !isError;
            buffers = new ArrayDeque<>(buffersRead);
            buffersRead.clear();
        }

        BufferUtils.recycleBuffers(buffers);
        if (notifyFailure) {
            ListenerUtils.notifyFailure(failureListener, throwable);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        Queue<Buffer> buffers;

        synchronized (lock) {
            isError = true;
            releaseCause = throwable;

            buffers = new ArrayDeque<>(buffersRead);
            buffersRead.clear();
        }

        BufferUtils.recycleBuffers(buffers);
    }

    /**
     * Closes this data partition reader which means all data has been read successfully. It should
     * notify failure and throw exception if encountering any failure.
     */
    protected void closeReader() throws Exception {
        synchronized (lock) {
            isFinished = true;
        }

        ListenerUtils.notifyAvailableData(dataListener);
    }

    @Override
    public boolean isFinished() {
        synchronized (lock) {
            return isFinished && buffersRead.isEmpty();
        }
    }
}
