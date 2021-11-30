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
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.storage.StorageMeta;

/**
 * A {@link LocalFileMapPartitionFactory} variant which only uses SSD to store data partition data.
 */
public class SSDOnlyLocalFileMapPartitionFactory extends LocalFileMapPartitionFactory {

    @Override
    public void initialize(Configuration configuration) {
        super.initialize(configuration);

        if (ssdStorageMetas.isEmpty()) {
            throw new ConfigurationException(
                    String.format(
                            "No valid data dir of SSD storage type is configured for %s.",
                            StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
        }
    }

    @Override
    protected StorageMeta getNextDataStorageMeta() {
        StorageMeta storageMeta = CommonUtils.checkNotNull(ssdStorageMetas.poll());
        ssdStorageMetas.add(storageMeta);
        return storageMeta;
    }
}
