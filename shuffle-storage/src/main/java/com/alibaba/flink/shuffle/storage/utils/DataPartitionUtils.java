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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.listener.PartitionStateListener;
import com.alibaba.flink.shuffle.core.storage.DataPartition;
import com.alibaba.flink.shuffle.core.storage.DataPartitionFactory;
import com.alibaba.flink.shuffle.core.storage.DataPartitionMeta;
import com.alibaba.flink.shuffle.core.storage.DataPartitionReader;
import com.alibaba.flink.shuffle.core.storage.DataPartitionWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/** Utility methods to manipulate {@link DataPartition}s. */
public class DataPartitionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DataPartitionUtils.class);

    /**
     * Helper method which releases the target {@link DataPartitionWriter} and logs the encountered
     * exception if any.
     */
    public static void releaseDataPartitionWriter(
            @Nullable DataPartitionWriter writer, @Nullable Throwable releaseCause) {
        if (writer == null) {
            return;
        }

        try {
            writer.release(releaseCause);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release data partition writer: {}.", writer, throwable);
        }
    }

    /**
     * Helper method which releases the target {@link DataPartitionReader} and logs the encountered
     * exception if any.
     */
    public static void releaseDataPartitionReader(
            @Nullable DataPartitionReader reader, @Nullable Throwable releaseCause) {
        if (reader == null) {
            return;
        }

        try {
            reader.release(releaseCause);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release data partition reader: {}.", reader, throwable);
        }
    }

    /**
     * Helper method which releases all the given {@link DataPartitionReader}s and logs the
     * encountered exception if any.
     */
    public static void releaseDataPartitionReaders(
            @Nullable Collection<DataPartitionReader> readers, @Nullable Throwable releaseCause) {
        if (readers == null) {
            return;
        }

        for (DataPartitionReader partitionReader : readers) {
            releaseDataPartitionReader(partitionReader, releaseCause);
        }
        // clear method is not supported by all collections
        CommonUtils.runQuietly(readers::clear);
    }

    /**
     * Helper method which releases the target {@link DataPartition} and logs the encountered
     * exception if any.
     */
    public static CompletableFuture<?> releaseDataPartition(
            @Nullable DataPartition dataPartition, @Nullable Throwable releaseCause) {
        if (dataPartition == null) {
            return CompletableFuture.completedFuture(null);
        }

        DataPartitionMeta partitionMeta = dataPartition.getPartitionMeta();
        try {
            return dataPartition.releasePartition(releaseCause);
        } catch (Throwable throwable) {
            LOG.error("Fatal: failed to release data partition: {}.", partitionMeta, throwable);
            CompletableFuture<?> future = new CompletableFuture<>();
            future.completeExceptionally(throwable);
            return future;
        }
    }

    /**
     * Helper method which releases all the given {@link DataPartition}s and logs the encountered
     * exception if any.
     */
    public static void releaseDataPartitions(
            @Nullable Collection<DataPartition> dataPartitions,
            @Nullable Throwable releaseCause,
            PartitionStateListener partitionStateListener) {
        if (dataPartitions == null) {
            return;
        }

        CommonUtils.checkArgument(partitionStateListener != null, "Must be not null.");
        for (DataPartition dataPartition : dataPartitions) {
            CommonUtils.runQuietly(
                    () -> {
                        releaseDataPartition(dataPartition, releaseCause).get();
                        partitionStateListener.onPartitionRemoved(dataPartition.getPartitionMeta());
                    },
                    true);
        }
        // clear method is not supported by all collections
        CommonUtils.runQuietly(dataPartitions::clear);
    }

    /**
     * Helper method which serializes the given {@link DataPartitionMeta} to the given {@link
     * DataOutput} which can be used to reconstruct lost {@link DataPartition}s.
     */
    public static void serializePartitionMeta(
            DataPartitionMeta partitionMeta, DataOutput dataOutput) throws Exception {
        dataOutput.writeUTF(partitionMeta.getPartitionFactoryClassName());
        partitionMeta.writeTo(dataOutput);
    }

    /**
     * Helper method which deserializes and creates a new {@link DataPartitionMeta} instance from
     * the given {@link DataInput}. The created {@link DataPartitionMeta} can be used to reconstruct
     * lost {@link DataPartition}s.
     */
    public static DataPartitionMeta deserializePartitionMeta(DataInput dataInput) throws Exception {
        String partitionFactoryClassName = dataInput.readUTF();
        Class<?> factoryClass = Class.forName(partitionFactoryClassName);
        DataPartitionFactory factory = (DataPartitionFactory) factoryClass.newInstance();
        return factory.recoverDataPartitionMeta(dataInput);
    }
}
