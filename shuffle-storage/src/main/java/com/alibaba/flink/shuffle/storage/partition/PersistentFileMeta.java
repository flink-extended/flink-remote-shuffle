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

import com.alibaba.flink.shuffle.common.config.Configuration;

import java.io.DataOutput;
import java.io.Serializable;

/** Meta information of {@link PersistentFile}. */
public interface PersistentFileMeta extends Serializable {

    /** Creates the corresponding {@link PersistentFile} instance represented by this file meta. */
    PersistentFile createPersistentFile(Configuration configuration);

    /**
     * Returns the storage version of the target {@link PersistentFile} for backward compatibility.
     */
    int getStorageVersion();

    /**
     * Writes all meta information to the target {@link DataOutput} which can be used to recover
     * data after failure.
     */
    void writeTo(DataOutput dataOutput) throws Exception;
}
