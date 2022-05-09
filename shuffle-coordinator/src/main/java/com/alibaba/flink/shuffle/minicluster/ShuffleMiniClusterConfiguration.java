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

package com.alibaba.flink.shuffle.minicluster;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.config.WorkerOptions;

import javax.annotation.Nullable;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Configuration object for the {@link ShuffleMiniCluster}. */
public class ShuffleMiniClusterConfiguration {

    private final Configuration configuration;

    private final int numShuffleWorkers;

    @Nullable private final String commonBindAddress;

    // ------------------------------------------------------------------------
    //  Construction
    // ------------------------------------------------------------------------

    public ShuffleMiniClusterConfiguration(
            Configuration configuration,
            int numShuffleWorkers,
            @Nullable String commonBindAddress) {

        this.numShuffleWorkers = numShuffleWorkers;
        this.configuration = checkNotNull(configuration);
        this.configuration.setInteger(
                ManagerOptions.RPC_BIND_PORT, ManagerOptions.RPC_PORT.defaultValue());
        this.commonBindAddress = commonBindAddress;
    }

    // ------------------------------------------------------------------------
    //  getters
    // ------------------------------------------------------------------------

    public int getNumShuffleWorkers() {
        return numShuffleWorkers;
    }

    public String getShuffleManagerExternalAddress() {
        return commonBindAddress != null
                ? commonBindAddress
                : configuration.getString(ManagerOptions.RPC_ADDRESS, "localhost");
    }

    public String getShuffleWorkerExternalAddress() {
        return commonBindAddress != null
                ? commonBindAddress
                : configuration.getString(WorkerOptions.HOST, "localhost");
    }

    public String getShuffleManagerExternalPortRange() {
        return String.valueOf(configuration.getInteger(ManagerOptions.RPC_PORT));
    }

    public String getShuffleWorkerExternalPortRange() {
        return configuration.getString(WorkerOptions.RPC_PORT);
    }

    public String getShuffleManagerBindAddress() {
        return commonBindAddress != null
                ? commonBindAddress
                : configuration.getString(ManagerOptions.RPC_BIND_ADDRESS, "localhost");
    }

    public String getShuffleWorkerBindAddress() {
        return commonBindAddress != null
                ? commonBindAddress
                : configuration.getString(WorkerOptions.BIND_HOST, "localhost");
    }

    public long getMinReservedSpaceBytes() {
        return configuration
                .getMemorySize(StorageOptions.STORAGE_MIN_RESERVED_SPACE_BYTES)
                .getBytes();
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public String toString() {
        return "MiniClusterConfiguration {"
                + "numShuffleWorker="
                + numShuffleWorkers
                + ", commonBindAddress='"
                + commonBindAddress
                + '\''
                + ", config="
                + configuration
                + '}';
    }

    // ----------------------------------------------------------------------------------
    // Enums
    // ----------------------------------------------------------------------------------

    // ----------------------------------------------------------------------------------
    // Builder
    // ----------------------------------------------------------------------------------

    /** Builder for the MiniClusterConfiguration. */
    public static class Builder {
        private Configuration configuration = new Configuration();
        private int numShuffleWorkers = 1;
        @Nullable private String commonBindAddress = null;

        public Builder setConfiguration(Configuration configuration) {
            this.configuration = checkNotNull(configuration);
            return this;
        }

        public Builder setNumShuffleWorkers(int numShuffleWorkers) {
            this.numShuffleWorkers = numShuffleWorkers;
            return this;
        }

        public Builder setCommonBindAddress(String commonBindAddress) {
            this.commonBindAddress = commonBindAddress;
            return this;
        }

        public ShuffleMiniClusterConfiguration build() {
            return new ShuffleMiniClusterConfiguration(
                    configuration, numShuffleWorkers, commonBindAddress);
        }
    }
}
