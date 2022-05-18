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

package com.alibaba.flink.shuffle.yarn.entry.manager;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This is the Yarn NM callback handler class. By implementing this interface, we can control the
 * job to execute the different logic corresponding to the different state.
 *
 * <p>This class does not implement special operations according to different stat, but we still
 * need to implement this interface {@link NMClientAsync.CallbackHandler} to transfer the job state
 * into the STARTED state.
 */
public class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(NMCallbackHandler.class);

    @Override
    public void onContainerStarted(
            ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        LOG.info("The " + containerId + " is started");
    }

    @Override
    public void onContainerStatusReceived(
            ContainerId containerId, ContainerStatus containerStatus) {
        LOG.info("Receive " + containerId + " status: " + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        LOG.info("The " + containerId + " is stopped");
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOG.info("Start " + containerId + " failed, ", t);
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        LOG.info("Get " + containerId + " status failed, ", t);
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        LOG.info("Stop " + containerId + " failed, ", t);
    }
}
