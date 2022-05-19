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

package com.alibaba.flink.shuffle.yarn.entry.manager;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * This is the Yarn RM callback handler class. By implementing this interface, we can control the
 * job to execute the different logic corresponding to the different state.
 *
 * <p>This class does not implement special operations according to different stat, but we still
 * need to implement this interface {@link AMRMClientAsync.CallbackHandler} to register container
 * with Yarn resource manager. After executing this class, it will continuously send heartbeat
 * signals to the resource manager, indicating that the current application is in Running state.
 */
public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RMCallbackHandler.class);

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        LOG.info(
                "All containers are completed, container status: "
                        + StringUtils.join(", ", statuses));
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        LOG.info("All containers are allocated, num: " + containers.size());
    }

    @Override
    public void onShutdownRequest() {
        LOG.info("Received shutdown request");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        LOG.info(
                "Nodes are updated and the node reports are "
                        + StringUtils.join(",", updatedNodes));
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        LOG.info("Encountered a error, ", e);
    }
}
