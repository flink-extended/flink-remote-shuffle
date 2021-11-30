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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/** Meta information of the data storage for {@link DataPartition}. */
public class StorageMeta implements Serializable {

    private static final long serialVersionUID = 7636731224603174535L;

    private final String storagePath;

    private final StorageType storageType;

    public StorageMeta(String storagePath, StorageType storageType) {
        CommonUtils.checkArgument(storagePath != null, "Must be not null.");
        CommonUtils.checkArgument(storageType != null, "Must be not null.");

        this.storagePath = storagePath;
        this.storageType = storageType;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public void writeTo(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(storageType.name());
        dataOutput.writeUTF(storagePath);
    }

    public static StorageMeta readFrom(DataInput dataInput) throws IOException {
        StorageType storageType = StorageType.valueOf(dataInput.readUTF());
        String storagePath = dataInput.readUTF();
        return new StorageMeta(storagePath, storageType);
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }

        if (that == null || getClass() != that.getClass()) {
            return false;
        }

        StorageMeta thatMeta = (StorageMeta) that;
        return Objects.equals(storagePath, thatMeta.storagePath)
                && storageType == thatMeta.storageType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(storagePath, storageType);
    }

    @Override
    public String toString() {
        return "StorageMeta{"
                + "StoragePath='"
                + storagePath
                + ", StorageType="
                + storageType
                + '}';
    }
}
