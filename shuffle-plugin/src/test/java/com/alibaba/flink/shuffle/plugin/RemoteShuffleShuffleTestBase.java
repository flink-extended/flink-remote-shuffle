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

package com.alibaba.flink.shuffle.plugin;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.HaServicesFactory;
import com.alibaba.flink.shuffle.coordinator.highavailability.LeaderInformation;
import com.alibaba.flink.shuffle.coordinator.highavailability.TestingHaServices;
import com.alibaba.flink.shuffle.coordinator.leaderretrieval.SettableLeaderRetrievalService;
import com.alibaba.flink.shuffle.coordinator.utils.TestingShuffleManagerGateway;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.rpc.test.TestingRpcService;
import com.alibaba.flink.shuffle.rpc.utils.RpcUtils;

import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import org.junit.After;
import org.junit.Before;

import java.util.concurrent.Executor;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Base class for the remote shuffle test. */
public class RemoteShuffleShuffleTestBase {

    public static final long TIMEOUT = 10000L;

    protected TestingRpcService rpcService;

    protected Configuration configuration;

    protected TestingFatalErrorHandler testingFatalErrorHandler;

    protected Executor mainThreadExecutor;

    protected TestingShuffleManagerGateway smGateway;

    @Before
    public void setup() throws Exception {
        rpcService = new TestingRpcService();

        configuration = new Configuration();
        configuration.setString(
                HighAvailabilityOptions.HA_MODE, TestingHaServiceFactory.class.getName());

        testingFatalErrorHandler = new TestingFatalErrorHandler();
        mainThreadExecutor = new DirectScheduledExecutorService();

        smGateway = new TestingShuffleManagerGateway();
        rpcService.registerGateway(smGateway.getAddress(), smGateway);

        TestingHaServiceFactory.shuffleManagerLeaderRetrieveService =
                new SettableLeaderRetrievalService();
        TestingHaServiceFactory.shuffleManagerLeaderRetrieveService.notifyListener(
                new LeaderInformation(smGateway.getFencingToken(), smGateway.getAddress()));
    }

    @After
    public void teardown() throws Exception {
        if (rpcService != null) {
            RpcUtils.terminateRpcService(rpcService, TIMEOUT);
            rpcService = null;
        }
    }

    /** A testing {@link HaServicesFactory} implementation. */
    public static class TestingHaServiceFactory implements HaServicesFactory {
        static SettableLeaderRetrievalService shuffleManagerLeaderRetrieveService;

        @Override
        public HaServices createHAServices(Configuration configuration) throws Exception {
            checkNotNull(shuffleManagerLeaderRetrieveService);
            TestingHaServices haServices = new TestingHaServices();
            haServices.setShuffleManagerLeaderRetrieveService(shuffleManagerLeaderRetrieveService);
            return haServices;
        }
    }
}
