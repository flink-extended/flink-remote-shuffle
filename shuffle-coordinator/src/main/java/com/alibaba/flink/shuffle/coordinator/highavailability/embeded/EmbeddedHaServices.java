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

package com.alibaba.flink.shuffle.coordinator.highavailability.embeded;

import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderElectionService;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderRetrievalService;

import javax.annotation.Nonnull;

import java.util.concurrent.Executor;

/**
 * An implementation of the {@link HaServices} for the non-high-availability case where all
 * participants run in the same process.
 *
 * <p>This implementation has no dependencies on any external services.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices).
 */
public class EmbeddedHaServices implements HaServices {

    protected final Object lock = new Object();

    private final EmbeddedLeaderService embeddedLeaderService;

    private boolean shutdown;

    public EmbeddedHaServices(Executor executor) {
        this.embeddedLeaderService = createEmbeddedLeaderService(executor);
    }

    @Override
    public LeaderRetrievalService createLeaderRetrievalService(LeaderReceptor receptor) {
        return embeddedLeaderService.createLeaderRetrievalService();
    }

    @Override
    public LeaderElectionService createLeaderElectionService() {
        return embeddedLeaderService.createLeaderElectionService();
    }

    @Nonnull
    private EmbeddedLeaderService createEmbeddedLeaderService(Executor executor) {
        return new EmbeddedLeaderService(executor);
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;
            }

            embeddedLeaderService.shutdown();
        }
    }

    @Override
    public void closeAndCleanupAllData() {
        this.close();
    }
}
