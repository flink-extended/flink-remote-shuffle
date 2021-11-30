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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.JvmShutdownSafeguard;
import com.alibaba.flink.shuffle.common.utils.SignalHandler;
import com.alibaba.flink.shuffle.coordinator.manager.entrypoint.ShuffleManagerEntrypoint;
import com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils;
import com.alibaba.flink.shuffle.coordinator.utils.EnvironmentInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.alibaba.flink.shuffle.coordinator.utils.ClusterEntrypointUtils.STARTUP_FAILURE_RETURN_CODE;

/** Runner for {@link ShuffleManager}. */
public class ShuffleManagerRunner {

    private static final Logger LOG = LoggerFactory.getLogger(ShuffleManagerRunner.class);

    public static void main(String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "Shuffle Manager", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if (maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        try {
            Configuration configuration = ClusterEntrypointUtils.parseParametersOrExit(args);
            ShuffleManagerEntrypoint shuffleManagerEntrypoint =
                    new ShuffleManagerEntrypoint(configuration);
            ShuffleManagerEntrypoint.runShuffleManagerEntrypoint(shuffleManagerEntrypoint);
        } catch (Throwable t) {
            LOG.error("ShuffleManager initialization failed.", t);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }
    }
}
