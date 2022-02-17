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

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;

/** Usable storage space information. */
@NotThreadSafe
public class UsableStorageSpaceInfo implements Serializable {

    public static final UsableStorageSpaceInfo ZERO_USABLE_SPACE = new UsableStorageSpaceInfo(0, 0);

    public static final UsableStorageSpaceInfo INFINITE_USABLE_SPACE =
            new UsableStorageSpaceInfo(Long.MAX_VALUE, Long.MAX_VALUE);

    private static final long serialVersionUID = 8761204897411057994L;

    private long ssdUsableSpaceBytes;

    private long hddUsableSpaceBytes;

    public UsableStorageSpaceInfo(long hddUsableSpaceBytes, long ssdUsableSpaceBytes) {
        this.hddUsableSpaceBytes = hddUsableSpaceBytes;
        this.ssdUsableSpaceBytes = ssdUsableSpaceBytes;
    }

    public void setHddUsableSpaceBytes(long hddUsableSpaceBytes) {
        this.hddUsableSpaceBytes = hddUsableSpaceBytes;
    }

    public void setSsdUsableSpaceBytes(long ssdUsableSpaceBytes) {
        this.ssdUsableSpaceBytes = ssdUsableSpaceBytes;
    }

    public long getHddUsableSpaceBytes() {
        return hddUsableSpaceBytes;
    }

    public long getSsdUsableSpaceBytes() {
        return ssdUsableSpaceBytes;
    }
}
