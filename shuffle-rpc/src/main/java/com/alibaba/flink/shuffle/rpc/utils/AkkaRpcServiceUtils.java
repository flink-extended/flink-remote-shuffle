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

package com.alibaba.flink.shuffle.rpc.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.FatalErrorExitUtils;
import com.alibaba.flink.shuffle.core.config.RpcOptions;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcServiceImpl;

import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.core.classloading.ComponentClassLoader;
import org.apache.flink.runtime.rpc.AddressResolution;
import org.apache.flink.runtime.rpc.RpcSystem;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.NetUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/**
 * These RPC utilities contain helper methods around RPC use, such as starting an RPC service, or
 * constructing RPC addresses.
 *
 * <p>This class is partly copied from Apache Flink
 * (org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils).
 */
public class AkkaRpcServiceUtils {

    private static final AtomicReference<RpcSystem> rpcSystemRef = new AtomicReference<>();

    private static final String SUPERVISOR_NAME = "rpc";

    private static final String AKKA_TCP = "akka.tcp";

    private static final String AKKA_SSL_TCP = "akka.ssl.tcp";

    private static final String actorSystemName = "remote-shuffle";

    private static final AtomicLong nextNameOffset = new AtomicLong(0L);

    public static RemoteShuffleRpcService createRemoteRpcService(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange,
            @Nullable String bindAddress,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<Integer> bindPort)
            throws Exception {
        final AkkaRpcServiceBuilder akkaRpcServiceBuilder =
                remoteServiceBuilder(configuration, externalAddress, externalPortRange);
        if (bindAddress != null) {
            akkaRpcServiceBuilder.withBindAddress(bindAddress);
        }
        bindPort.ifPresent(akkaRpcServiceBuilder::withBindPort);
        return akkaRpcServiceBuilder.createAndStart();
    }

    public static AkkaRpcServiceBuilder remoteServiceBuilder(
            Configuration configuration,
            @Nullable String externalAddress,
            String externalPortRange) {
        return new AkkaRpcServiceBuilder(configuration, externalAddress, externalPortRange);
    }

    /**
     * @param hostname The hostname or address where the target RPC service is listening.
     * @param port The port where the target RPC service is listening.
     * @param endpointName The name of the RPC endpoint.
     * @param akkaProtocol True, if security/encryption is enabled, false otherwise.
     * @return The RPC URL of the specified RPC endpoint.
     */
    public static String getRpcUrl(
            String hostname, int port, String endpointName, AkkaProtocol akkaProtocol) {

        checkArgument(hostname != null, "Hostname is null.");
        checkArgument(endpointName != null, "EndpointName is null.");
        checkArgument(NetUtils.isValidClientPort(port), "Port must be in [1, 65535]");

        String hostPort = NetUtils.unresolvedHostAndPortToNormalizedString(hostname, port);
        return internalRpcUrl(endpointName, new RemoteAddressInformation(hostPort, akkaProtocol));
    }

    private static String internalRpcUrl(
            String endpointName, RemoteAddressInformation remoteAddressInformation) {
        String protocolPrefix = akkaProtocolToString(remoteAddressInformation.getAkkaProtocol());
        String hostPort = remoteAddressInformation.getHostnameAndPort();

        // protocolPrefix://flink-remote-shuffle[@hostname:port]/user/rpc/endpointName
        return String.format("%s://" + actorSystemName, protocolPrefix)
                + "@"
                + hostPort
                + "/user/"
                + SUPERVISOR_NAME
                + "/"
                + endpointName;
    }

    private static String akkaProtocolToString(AkkaProtocol akkaProtocol) {
        return akkaProtocol == AkkaProtocol.SSL_TCP ? AKKA_SSL_TCP : AKKA_TCP;
    }

    public static InetSocketAddress getInetSocketAddressFromAkkaURL(String akkaURL)
            throws Exception {
        checkState(rpcSystemRef.get() != null, "Rpc system is not initialized.");
        return rpcSystemRef.get().getInetSocketAddressFromRpcUrl(akkaURL);
    }

    /**
     * Creates a random name of the form prefix_X, where X is an increasing number.
     *
     * @param prefix Prefix string to prepend to the monotonically increasing name offset number
     * @return A random name of the form prefix_X where X is an increasing number
     */
    public static String createRandomName(String prefix) {
        CommonUtils.checkArgument(prefix != null, "Prefix must not be null.");

        long nameOffset;

        // obtain the next name offset by incrementing it atomically
        do {
            nameOffset = nextNameOffset.get();
        } while (!nextNameOffset.compareAndSet(nameOffset, nameOffset + 1L));

        return prefix + '_' + nameOffset;
    }

    /**
     * Creates a wildcard name symmetric to {@link #createRandomName(String)}.
     *
     * @param prefix prefix of the wildcard name
     * @return wildcard name starting with the prefix
     */
    public static String createWildcardName(String prefix) {
        return prefix + "_*";
    }

    /** Whether to use TCP or encrypted TCP for Akka. */
    public enum AkkaProtocol {
        TCP,
        SSL_TCP
    }

    private static final class RemoteAddressInformation {
        private final String hostnameAndPort;
        private final AkkaProtocol akkaProtocol;

        private RemoteAddressInformation(String hostnameAndPort, AkkaProtocol akkaProtocol) {
            this.hostnameAndPort = hostnameAndPort;
            this.akkaProtocol = akkaProtocol;
        }

        private String getHostnameAndPort() {
            return hostnameAndPort;
        }

        private AkkaProtocol getAkkaProtocol() {
            return akkaProtocol;
        }
    }

    /** Builder for {@link RemoteShuffleRpcService}. */
    public static class AkkaRpcServiceBuilder {

        private final org.apache.flink.configuration.Configuration flinkConf;

        @Nullable private final String externalAddress;
        @Nullable private final String externalPortRange;

        private String actorSystemName = AkkaRpcServiceUtils.actorSystemName;

        private final RpcSystem.ForkJoinExecutorConfiguration forkJoinExecutorConfiguration;

        private String bindAddress = NetUtils.getWildcardIPAddress();
        @Nullable private Integer bindPort = null;

        /** Builder for creating a remote RPC service. */
        private AkkaRpcServiceBuilder(
                Configuration configuration,
                @Nullable String externalAddress,
                @Nullable String externalPortRange) {
            CommonUtils.checkArgument(configuration != null, "Must be not null.");
            CommonUtils.checkArgument(externalPortRange != null, "Must be not null.");

            this.flinkConf =
                    org.apache.flink.configuration.Configuration.fromMap(configuration.toMap());
            // convert remote shuffle configuration to flink configuration
            flinkConf.set(
                    AkkaOptions.ASK_TIMEOUT_DURATION,
                    configuration.getDuration(RpcOptions.RPC_TIMEOUT));
            flinkConf.set(
                    AkkaOptions.FRAMESIZE, configuration.getString(RpcOptions.AKKA_FRAME_SIZE));
            if (!FatalErrorExitUtils.isNeedStopProcess()) {
                flinkConf.set(AkkaOptions.JVM_EXIT_ON_FATAL_ERROR, false);
            }

            this.externalAddress =
                    externalAddress == null
                            ? InetAddress.getLoopbackAddress().getHostAddress()
                            : externalAddress;
            this.externalPortRange = externalPortRange;
            this.forkJoinExecutorConfiguration = getForkJoinExecutorConfiguration(flinkConf);
        }

        public AkkaRpcServiceBuilder withActorSystemName(String actorSystemName) {
            this.actorSystemName = CommonUtils.checkNotNull(actorSystemName);
            return this;
        }

        public AkkaRpcServiceBuilder withBindAddress(String bindAddress) {
            this.bindAddress = CommonUtils.checkNotNull(bindAddress);
            return this;
        }

        public AkkaRpcServiceBuilder withBindPort(int bindPort) {
            CommonUtils.checkArgument(
                    NetUtils.isValidHostPort(bindPort), "Invalid port number: " + bindPort);
            this.bindPort = bindPort;
            return this;
        }

        public RemoteShuffleRpcService createAndStart() throws Exception {
            if (rpcSystemRef.get() == null) {
                loadRpcSystem(new Configuration());
            }

            RpcSystem.RpcServiceBuilder rpcServiceBuilder;
            if (externalAddress == null) {
                // create local actor system
                rpcServiceBuilder = rpcSystemRef.get().localServiceBuilder(flinkConf);
            } else {
                // create remote actor system
                rpcServiceBuilder =
                        rpcSystemRef
                                .get()
                                .remoteServiceBuilder(
                                        flinkConf, externalAddress, externalPortRange);
            }

            rpcServiceBuilder
                    .withComponentName(actorSystemName)
                    .withBindAddress(bindAddress)
                    .withExecutorConfiguration(forkJoinExecutorConfiguration);
            if (bindPort != null) {
                rpcServiceBuilder.withBindPort(bindPort);
            }
            return new RemoteShuffleRpcServiceImpl(rpcServiceBuilder.createAndStart());
        }
    }

    public static RpcSystem.ForkJoinExecutorConfiguration getForkJoinExecutorConfiguration(
            org.apache.flink.configuration.Configuration configuration) {
        double parallelismFactor =
                configuration.getDouble(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR);
        int minParallelism =
                configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN);
        int maxParallelism =
                configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX);

        return new RpcSystem.ForkJoinExecutorConfiguration(
                parallelismFactor, minParallelism, maxParallelism);
    }

    public static void loadRpcSystem(Configuration configuration) {
        try {
            if (rpcSystemRef.get() != null) {
                return;
            }

            org.apache.flink.configuration.Configuration flinkConf =
                    org.apache.flink.configuration.Configuration.fromMap(configuration.toMap());
            ClassLoader flinkClassLoader = RpcSystem.class.getClassLoader();

            Path tmpDirectory = Paths.get(ConfigurationUtils.parseTempDirectories(flinkConf)[0]);
            Files.createDirectories(tmpDirectory);
            Path tempFile =
                    Files.createFile(
                            tmpDirectory.resolve("flink-rpc-akka_" + UUID.randomUUID() + ".jar"));

            boolean isShaded = RpcSystem.class.getName().startsWith("com.alibaba.flink.shuffle");
            String rpcJarName = isShaded ? "shaded-flink-rpc-akka.jar" : "flink-rpc-akka.jar";
            InputStream resourceStream = flinkClassLoader.getResourceAsStream(rpcJarName);
            if (resourceStream == null) {
                throw new RuntimeException("Akka RPC system could not be found.");
            }

            IOUtils.copyBytes(resourceStream, Files.newOutputStream(tempFile));

            ComponentClassLoader classLoader =
                    new ComponentClassLoader(
                            new URL[] {tempFile.toUri().toURL()},
                            flinkClassLoader,
                            CoreOptions.parseParentFirstLoaderPatterns(
                                    CoreOptions.PARENT_FIRST_LOGGING_PATTERNS, ""),
                            new String[] {
                                isShaded ? "org.apache.flink.shaded" : "org.apache.flink",
                                "com.alibaba.flink.shuffle"
                            });

            RpcSystem newRpcSystem =
                    new CleanupOnCloseRpcSystem(
                            ServiceLoader.load(RpcSystem.class, classLoader).iterator().next(),
                            classLoader,
                            tempFile);

            if (!rpcSystemRef.compareAndSet(null, newRpcSystem)) {
                newRpcSystem.close();
            } else {
                Runtime.getRuntime()
                        .addShutdownHook(new Thread(AkkaRpcServiceUtils::closeRpcSystem));
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not initialize RPC system.", e);
        }
    }

    public static void closeRpcSystem() {
        RpcSystem rpcSystem = rpcSystemRef.get();
        if (rpcSystem != null && rpcSystemRef.compareAndSet(rpcSystem, null)) {
            rpcSystem.close();
        }
    }

    /**
     * This is copied from Apache Flink (org.apache.flink.runtime.rpc.akka.CleanupOnCloseRpcSystem).
     */
    private static final class CleanupOnCloseRpcSystem implements RpcSystem {
        private static final Logger LOG = LoggerFactory.getLogger(CleanupOnCloseRpcSystem.class);

        private final RpcSystem rpcSystem;
        private final ComponentClassLoader classLoader;
        private final Path tempFile;

        public CleanupOnCloseRpcSystem(
                RpcSystem rpcSystem, ComponentClassLoader classLoader, Path tempFile) {
            this.rpcSystem = CommonUtils.checkNotNull(rpcSystem);
            this.classLoader = CommonUtils.checkNotNull(classLoader);
            this.tempFile = CommonUtils.checkNotNull(tempFile);
        }

        @Override
        public void close() {
            rpcSystem.close();

            try {
                classLoader.close();
            } catch (Exception e) {
                LOG.warn("Could not close RpcSystem classloader.", e);
            }
            try {
                Files.delete(tempFile);
            } catch (Exception e) {
                LOG.warn("Could not delete temporary rpc system file {}.", tempFile, e);
            }
        }

        @Override
        public RpcServiceBuilder localServiceBuilder(
                org.apache.flink.configuration.Configuration config) {
            return rpcSystem.localServiceBuilder(config);
        }

        @Override
        public RpcServiceBuilder remoteServiceBuilder(
                org.apache.flink.configuration.Configuration configuration,
                @Nullable String externalAddress,
                String externalPortRange) {
            return rpcSystem.remoteServiceBuilder(
                    configuration, externalAddress, externalPortRange);
        }

        @Override
        public String getRpcUrl(
                String hostname,
                int port,
                String endpointName,
                AddressResolution addressResolution,
                org.apache.flink.configuration.Configuration config)
                throws UnknownHostException {
            return rpcSystem.getRpcUrl(hostname, port, endpointName, addressResolution, config);
        }

        @Override
        public InetSocketAddress getInetSocketAddressFromRpcUrl(String url) throws Exception {
            return rpcSystem.getInetSocketAddressFromRpcUrl(url);
        }

        @Override
        public long getMaximumMessageSizeInBytes(
                org.apache.flink.configuration.Configuration config) {
            return rpcSystem.getMaximumMessageSizeInBytes(config);
        }
    }
}
