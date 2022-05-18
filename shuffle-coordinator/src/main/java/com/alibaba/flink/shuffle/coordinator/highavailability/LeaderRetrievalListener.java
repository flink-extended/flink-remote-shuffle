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

package com.alibaba.flink.shuffle.coordinator.highavailability;

/**
 * Classes which want to be notified about a changing leader by the {@link LeaderRetrievalService}
 * have to implement this interface.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener).
 */
public interface LeaderRetrievalListener {

    /**
     * This method is called by the {@link LeaderRetrievalService} when a new leader is elected.
     *
     * <p>If both arguments are null then it signals that leadership was revoked without a new
     * leader having been elected.
     */
    void notifyLeaderAddress(LeaderInformation leaderInfo);

    /**
     * This method is called by the {@link LeaderRetrievalService} in case of an exception. This
     * assures that the {@link LeaderRetrievalListener} is aware of any problems occurring in the
     * {@link LeaderRetrievalService} thread.
     */
    void handleError(Exception exception);
}
