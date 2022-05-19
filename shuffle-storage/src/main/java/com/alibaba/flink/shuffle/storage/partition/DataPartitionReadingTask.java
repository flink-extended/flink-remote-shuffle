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

package com.alibaba.flink.shuffle.storage.partition;

import javax.annotation.Nullable;

/**
 * {@link DataPartitionReadingTask} encapsulates the logic of data partition reading and will be
 * triggered when there is a data reading request.
 */
public interface DataPartitionReadingTask extends PartitionProcessingTask {

    /** Allocates resources for data reading, will be called on the first data reading request. */
    void allocateResources() throws Exception;

    /** Triggers running of this reading task which will read data from data partition. */
    void triggerReading();

    /** Releases this {@link DataPartitionReadingTask} which releases all allocated resources. */
    void release(@Nullable Throwable throwable) throws Exception;
}
