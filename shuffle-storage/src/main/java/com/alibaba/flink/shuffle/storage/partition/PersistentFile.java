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

/** {@link PersistentFile} is the interface for persistent data partition file. */
public interface PersistentFile {

    /**
     * Returns the latest storage version of this persistent file. This is for storage format
     * evolution and backward compatibility.
     */
    int getLatestStorageVersion();

    /** Checks whether this persistent file is consumable or not and returns true if so. */
    boolean isConsumable();

    /** Returns the total length in bytes, including data file and index file. */
    long totalBytes();

    /** Gets the corresponding meta of this persistent file. */
    PersistentFileMeta getFileMeta();

    /** Deletes this persistent file and throws the exception if any failure occurs. */
    void deleteFile() throws Exception;

    /** Notifies that an error happens while reading data from this persistent file. */
    void onError(Throwable throwable);

    /** Changes the consumable state of this persistent file. */
    void setConsumable(boolean consumable);

    /**
     * Increments the total length of this data partition in bytes, including data files and index
     * files.
     */
    void incrementTotalBytes(long incrementBytes);
}
