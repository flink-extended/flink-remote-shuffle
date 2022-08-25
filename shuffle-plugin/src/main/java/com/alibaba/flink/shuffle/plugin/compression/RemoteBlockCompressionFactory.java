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

package com.alibaba.flink.shuffle.plugin.compression;

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.compression.BlockDecompressor;

import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * Each compression codec has a implementation of {@link
 * org.apache.flink.runtime.io.compression.BlockCompressionFactory} to create compressors and
 * decompressors.
 */
public interface RemoteBlockCompressionFactory {

    BlockCompressor getCompressor();

    BlockDecompressor getDecompressor();

    /** Name of {@link org.apache.flink.runtime.io.compression.BlockCompressionFactory}. */
    enum CompressionFactoryName {
        LZ4,
        LZO,
        ZSTD
    }

    /**
     * Creates {@link org.apache.flink.runtime.io.compression.BlockCompressionFactory} according to
     * the configuration.
     *
     * @param compressionFactoryName supported compression codecs or user-defined class name
     *     inherited from {@link org.apache.flink.runtime.io.compression.BlockCompressionFactory}.
     */
    static RemoteBlockCompressionFactory createBlockCompressionFactory(
            String compressionFactoryName) {

        checkNotNull(compressionFactoryName);

        CompressionFactoryName compressionName;
        try {
            compressionName = CompressionFactoryName.valueOf(compressionFactoryName.toUpperCase());
        } catch (IllegalArgumentException e) {
            compressionName = null;
        }

        RemoteBlockCompressionFactory blockCompressionFactory;
        if (compressionName != null) {
            switch (compressionName) {
                case LZ4:
                    blockCompressionFactory = new Lz4BlockCompressionFactory();
                    break;
                case LZO:
                    blockCompressionFactory =
                            new AirCompressorFactory(new LzoCompressor(), new LzoDecompressor());
                    break;
                case ZSTD:
                    blockCompressionFactory =
                            new AirCompressorFactory(new ZstdCompressor(), new ZstdDecompressor());
                    break;
                default:
                    throw new IllegalStateException("Unknown CompressionMethod " + compressionName);
            }
        } else {
            Object factoryObj;
            try {
                factoryObj = Class.forName(compressionFactoryName).newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalConfigurationException(
                        "Cannot load class " + compressionFactoryName, e);
            } catch (Exception e) {
                throw new IllegalConfigurationException(
                        "Cannot create object for class " + compressionFactoryName, e);
            }
            if (factoryObj instanceof RemoteBlockCompressionFactory) {
                blockCompressionFactory = (RemoteBlockCompressionFactory) factoryObj;
            } else {
                throw new IllegalArgumentException(
                        "CompressionFactoryName should inherit from"
                                + " interface BlockCompressionFactory, or use the default compression codec.");
            }
        }

        checkNotNull(blockCompressionFactory);
        return blockCompressionFactory;
    }
}
