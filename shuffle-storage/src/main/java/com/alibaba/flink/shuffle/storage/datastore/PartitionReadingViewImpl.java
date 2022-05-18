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

package com.alibaba.flink.shuffle.storage.datastore;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.storage.BufferWithBacklog;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReadingView;

/** Implementation of {@link DataPartitionReadingView}. */
public class PartitionReadingViewImpl implements DataPartitionReadingView {

    /** The corresponding {@link DataPartitionReader} to read data from. */
    private final DataPartitionReader reader;

    private boolean isError;

    public PartitionReadingViewImpl(DataPartitionReader reader) {
        CommonUtils.checkArgument(reader != null, "Must be not null.");
        this.reader = reader;
    }

    @Override
    public BufferWithBacklog nextBuffer() throws Exception {
        checkNotInErrorState();

        return reader.nextBuffer();
    }

    @Override
    public void onError(Throwable throwable) {
        checkNotInErrorState();

        isError = true;
        reader.onError(throwable);
    }

    @Override
    public boolean isFinished() {
        checkNotInErrorState();

        return reader.isFinished();
    }

    private void checkNotInErrorState() {
        CommonUtils.checkState(!isError, "Reading view is in error state.");
    }
}
