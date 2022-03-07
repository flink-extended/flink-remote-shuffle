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

package com.alibaba.flink.shuffle.storage.partition;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ExceptionUtils;
import com.alibaba.flink.shuffle.core.listener.BacklogListener;
import com.alibaba.flink.shuffle.core.listener.DataListener;
import com.alibaba.flink.shuffle.core.listener.FailureListener;
import com.alibaba.flink.shuffle.core.memory.Buffer;
import com.alibaba.flink.shuffle.core.memory.BufferRecycler;
import com.alibaba.flink.shuffle.core.storage.BufferQueue;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.utils.BufferUtils;
import com.alibaba.flink.shuffle.core.utils.ListenerUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/** {@link DataPartitionReader} for {@link LocalFileReducePartition}. */
public class LocalFileReducePartitionReader extends BaseDataPartitionReader {
    private static final Logger LOG = LoggerFactory.getLogger(LocalFileReducePartitionReader.class);

    /** File reader responsible for reading data from partition file. */
    private final LocalReducePartitionFileReader fileReader;

    /** Whether this {@link DataPartitionReader} has been opened or not. */
    private boolean isOpened;

    public LocalFileReducePartitionReader(
            LocalReducePartitionFileReader fileReader,
            DataListener dataListener,
            BacklogListener backlogListener,
            FailureListener failureListener) {
        super(dataListener, backlogListener, failureListener);

        CommonUtils.checkArgument(fileReader != null, "Must be not null.");
        this.fileReader = fileReader;
    }

    @Override
    public void open() throws Exception {
        CommonUtils.checkState(!isOpened, "Partition reader has been opened.");

        isOpened = true;
        fileReader.open();
    }

    /**
     * Reads data through the corresponding {@link LocalReducePartitionFileReader} and returns true
     * if there is remaining data to be read with this data partition reader.
     */
    @Override
    public boolean readData(BufferQueue buffers, BufferRecycler recycler) throws Exception {
        CommonUtils.checkArgument(buffers != null, "Must be not null.");
        CommonUtils.checkArgument(recycler != null, "Must be not null.");

        CommonUtils.checkState(isOpened, "Partition reader is not opened.");

        boolean hasReaming = fileReader.hasRemaining();
        boolean continueReading = hasReaming;
        int numDataBuffers = 0;
        while (continueReading) {
            ByteBuffer buffer = buffers.poll();
            if (buffer == null) {
                break;
            }

            try {
                continueReading = fileReader.readBuffer(buffer);
            } catch (Throwable throwable) {
                BufferUtils.recycleBuffer(buffer, recycler);
                throw throwable;
            }

            hasReaming = fileReader.hasRemaining();
            addBuffer(new Buffer(buffer, recycler, buffer.remaining()), hasReaming);
            ++numDataBuffers;
        }

        LOG.debug(
                "The backlog num of data buffers is {}, buffersRead={}",
                numDataBuffers,
                buffersRead.size());

        if (numDataBuffers > 0) {
            notifyBacklog(numDataBuffers);
        }

        if (!hasReaming) {
            closeReader();
        }

        return hasReaming;
    }

    @Override
    public long getPriority() {
        CommonUtils.checkState(isOpened, "Partition reader is not opened.");

        // small file offset has high reading priority, it means when reading the partition file,
        // the reader always reads data in file offset order which can reduce random IO and lead to
        // more sequential reading thus is better for IO performance
        return fileReader.geConsumingOffset();
    }

    @Override
    public boolean isOpened() {
        return isOpened;
    }

    @Override
    public void release(Throwable releaseCause) throws Exception {
        Throwable exception = null;

        try {
            super.release(releaseCause);
        } catch (Throwable throwable) {
            exception = throwable;
            LOG.error("Failed to release base partition reader.", throwable);
        }

        try {
            fileReader.close();
        } catch (Throwable throwable) {
            exception = exception != null ? exception : throwable;
            LOG.error("Failed to release file reader.", throwable);
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(exception);
        }
    }

    @Override
    protected void closeReader() throws Exception {
        try {
            fileReader.finishReading();

            // mark the reader as finished after closing succeeded
            super.closeReader();
        } catch (Throwable throwable) {
            ListenerUtils.notifyFailure(failureListener, throwable);
            ExceptionUtils.rethrowException(throwable);
        }
    }
}
