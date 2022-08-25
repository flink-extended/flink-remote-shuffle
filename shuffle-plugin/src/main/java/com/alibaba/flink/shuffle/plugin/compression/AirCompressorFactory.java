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

import org.apache.flink.runtime.io.compression.BlockCompressor;
import org.apache.flink.runtime.io.compression.BlockDecompressor;

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;

/**
 * {@link RemoteBlockCompressionFactory} to create wrapped {@link Compressor} and {@link
 * Decompressor}.
 */
public class AirCompressorFactory implements RemoteBlockCompressionFactory {
    private final Compressor internalCompressor;

    private final Decompressor internalDecompressor;

    public AirCompressorFactory(Compressor internalCompressor, Decompressor internalDecompressor) {
        this.internalCompressor = internalCompressor;
        this.internalDecompressor = internalDecompressor;
    }

    @Override
    public BlockCompressor getCompressor() {
        return new AirBlockCompressor(internalCompressor);
    }

    @Override
    public BlockDecompressor getDecompressor() {
        return new AirBlockDecompressor(internalDecompressor);
    }
}
