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

import java.util.Objects;

/** Statistics information of {@link PersistentFile}. */
public class PersistentFileStatistics {

    private final long numDataRegions;

    private final long indexFileBytes;

    private final long dataFileBytes;

    public PersistentFileStatistics(long numDataRegions, long indexFileBytes, long dataFileBytes) {
        this.numDataRegions = numDataRegions;
        this.indexFileBytes = indexFileBytes;
        this.dataFileBytes = dataFileBytes;
    }

    public long getNumDataRegions() {
        return numDataRegions;
    }

    public long getIndexFileBytes() {
        return indexFileBytes;
    }

    public long getDataFileBytes() {
        return dataFileBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PersistentFileStatistics that = (PersistentFileStatistics) o;
        return numDataRegions == that.numDataRegions
                && indexFileBytes == that.indexFileBytes
                && dataFileBytes == that.dataFileBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numDataRegions, indexFileBytes, dataFileBytes);
    }
}
