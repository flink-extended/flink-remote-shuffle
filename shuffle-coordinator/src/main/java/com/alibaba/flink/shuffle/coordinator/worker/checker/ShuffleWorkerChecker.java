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

package com.alibaba.flink.shuffle.coordinator.worker.checker;

import com.alibaba.flink.shuffle.core.storage.DataStoreStatistics;
import com.alibaba.flink.shuffle.core.storage.StorageSpaceInfo;

/**
 * A interface to achieve the state of the shuffle worker, including storage state, the information
 * of data partition files, etc.
 */
public interface ShuffleWorkerChecker {

    /** Gets the storage space information of the target shuffle worker. */
    StorageSpaceInfo getStorageSpaceInfo();

    /** Gets the statistics information of the corresponding data store. */
    DataStoreStatistics getDataStoreStatistics();

    /** Close the shuffle worker checker. */
    void close();
}
