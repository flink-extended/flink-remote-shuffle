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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.handler.FatalErrorHandler;

/**
 * Factory for creating {@link LeaderElectionDriver} with different implementation.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderelection.LeaderElectionDriverFactory).
 */
public interface LeaderElectionDriverFactory {

    /**
     * Create a specific {@link LeaderElectionDriver} and start the necessary services. For example,
     * LeaderLatch and NodeCache in Zookeeper, KubernetesLeaderElector and ConfigMap watcher in
     * Kubernetes.
     *
     * @param leaderEventHandler handler for the leader election driver to process leader events.
     * @param leaderContenderDescription leader contender description.
     * @param fatalErrorHandler fatal error handler
     * @throws Exception when create a specific {@link LeaderElectionDriver} implementation and
     *     start the necessary services.
     */
    LeaderElectionDriver createLeaderElectionDriver(
            LeaderElectionEventHandler leaderEventHandler,
            FatalErrorHandler fatalErrorHandler,
            String leaderContenderDescription)
            throws Exception;
}
