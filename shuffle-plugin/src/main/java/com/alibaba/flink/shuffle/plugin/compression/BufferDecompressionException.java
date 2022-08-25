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

/**
 * A {@code BufferDecompressionException} is thrown when the target data cannot be decompressed,
 * such as data corruption, insufficient target buffer space for decompression, etc.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.io.compression.BufferDecompressionException).
 */
public class BufferDecompressionException extends RuntimeException {

    public BufferDecompressionException() {
        super();
    }

    public BufferDecompressionException(String message) {
        super(message);
    }

    public BufferDecompressionException(String message, Throwable e) {
        super(message, e);
    }

    public BufferDecompressionException(Throwable e) {
        super(e);
    }
}
