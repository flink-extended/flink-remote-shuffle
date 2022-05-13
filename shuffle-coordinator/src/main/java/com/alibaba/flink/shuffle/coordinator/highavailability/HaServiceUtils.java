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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.common.utils.NetUtils;
import com.alibaba.flink.shuffle.coordinator.highavailability.embeded.EmbeddedHaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.standalone.StandaloneHaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperHaServices;
import com.alibaba.flink.shuffle.coordinator.highavailability.zookeeper.ZooKeeperUtils;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.Executor;

/** Utils class to instantiate {@link HaServices} implementations. */
public class HaServiceUtils {

    private static final String SHUFFLE_MANAGER_NAME = "shufflemanager";

    /** Used by tests only. */
    public static HaServices createAvailableOrEmbeddedServices(
            Configuration config, Executor executor) throws Exception {

        HaMode highAvailabilityMode = HaMode.fromConfig(config);

        switch (highAvailabilityMode) {
            case NONE:
                return new EmbeddedHaServices(executor);
            case ZOOKEEPER:
                return new ZooKeeperHaServices(
                        config, ZooKeeperUtils.startCuratorFramework(config));
            case FACTORY_CLASS:
                return createCustomHAServices(config);
            default:
                throw new Exception(
                        "High availability mode " + highAvailabilityMode + " is not supported.");
        }
    }

    public static HaServices createHAServices(Configuration config) throws Exception {
        HaMode highAvailabilityMode = HaMode.fromConfig(config);
        switch (highAvailabilityMode) {
            case NONE:
                final Pair<String, Integer> hostnamePort = getShuffleManagerAddress(config);

                final String shuffleManagerRpcUrl =
                        AkkaRpcServiceUtils.getRpcUrl(
                                hostnamePort.getLeft(),
                                hostnamePort.getRight(),
                                AkkaRpcServiceUtils.createWildcardName(SHUFFLE_MANAGER_NAME),
                                AkkaRpcServiceUtils.AkkaProtocol.TCP);
                return new StandaloneHaServices(shuffleManagerRpcUrl);
            case ZOOKEEPER:
                return new ZooKeeperHaServices(
                        config, ZooKeeperUtils.startCuratorFramework(config));
            case FACTORY_CLASS:
                return createCustomHAServices(config);
            default:
                throw new Exception("Recovery mode " + highAvailabilityMode + " is not supported.");
        }
    }

    /**
     * Returns the ShuffleManager's hostname and port extracted from the given {@link
     * org.apache.flink.configuration.Configuration}.
     *
     * @param configuration Configuration to extract the ShuffleManager's address from
     * @return The ShuffleManager's hostname and port
     * @throws ConfigurationException if the ShuffleManager's address cannot be extracted from the
     *     configuration
     */
    public static Pair<String, Integer> getShuffleManagerAddress(Configuration configuration)
            throws ConfigurationException {

        final String hostname = configuration.getString(ManagerOptions.RPC_ADDRESS);
        final int port = configuration.getInteger(ManagerOptions.RPC_PORT);

        if (hostname == null) {
            throw new ConfigurationException(
                    "Config parameter '"
                            + ManagerOptions.RPC_ADDRESS.key()
                            + "' is missing (hostname/address of ShuffleManager to connect to).");
        }

        if (!NetUtils.isValidHostPort(port)) {
            throw new ConfigurationException(
                    "Invalid value for '"
                            + ManagerOptions.RPC_PORT.key()
                            + "' (port of the ShuffleManager actor system) : "
                            + port
                            + ".  it must be greater than 0 and less than 65536.");
        }

        return Pair.of(hostname, port);
    }

    private static HaServices createCustomHAServices(Configuration config) throws Exception {
        Class<?> clazz = Class.forName(config.getString(HighAvailabilityOptions.HA_MODE));
        HaServicesFactory haServicesFactory = (HaServicesFactory) clazz.newInstance();

        try {
            return haServicesFactory.createHAServices(config);
        } catch (Exception e) {
            throw new Exception(
                    String.format(
                            "Could not create the ha services from the instantiated HighAvailabilityServicesFactory %s.",
                            haServicesFactory.getClass().getName()),
                    e);
        }
    }
}
