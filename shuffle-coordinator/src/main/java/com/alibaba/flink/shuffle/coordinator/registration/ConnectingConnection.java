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

package com.alibaba.flink.shuffle.coordinator.registration;

import com.alibaba.flink.shuffle.coordinator.manager.RegistrationSuccess;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcGateway;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;

import org.slf4j.Logger;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** The connecting connection to one rpc address. */
public class ConnectingConnection<G extends RemoteShuffleRpcGateway, S extends RegistrationSuccess>
        extends RegisteredRpcConnection<UUID, G, S> {

    /** The name of the target. */
    private final String targetName;

    /** The class of the rpc gateway. */
    private final Class<G> targetType;

    /** The rpc service used to connect to the rpc address. */
    private final RemoteShuffleRpcService rpcService;

    /** The configuration for registration. */
    private final RetryingRegistrationConfiguration retryingRegistrationConfiguration;

    /** The listener when connecting succeed. */
    private final RegistrationConnectionListener<ConnectingConnection<G, S>, S>
            registrationListener;

    private final Function<G, CompletableFuture<RegistrationResponse>> registrationFunction;

    public ConnectingConnection(
            Logger log,
            String targetName,
            Class<G> targetType,
            RemoteShuffleRpcService rpcService,
            RetryingRegistrationConfiguration retryingRegistrationConfiguration,
            String targetAddress,
            UUID leaderId,
            Executor executor,
            RegistrationConnectionListener<ConnectingConnection<G, S>, S> registrationListener,
            Function<G, CompletableFuture<RegistrationResponse>> registrationFunction) {

        super(log, targetAddress, leaderId, executor);
        this.targetName = checkNotNull(targetName);
        this.targetType = checkNotNull(targetType);
        this.rpcService = checkNotNull(rpcService);
        this.retryingRegistrationConfiguration = checkNotNull(retryingRegistrationConfiguration);
        this.registrationListener = checkNotNull(registrationListener);
        this.registrationFunction = checkNotNull(registrationFunction);
    }

    @Override
    protected RetryingRegistration<UUID, G, S> generateRegistration() {
        return new RpcTargetRegistration(
                log,
                rpcService,
                targetName,
                targetType,
                getTargetAddress(),
                getTargetLeaderId(),
                retryingRegistrationConfiguration,
                registrationFunction);
    }

    @Override
    protected void onRegistrationSuccess(S success) {
        log.info("Successful registration at shuffle manager {}", getTargetAddress());

        registrationListener.onRegistrationSuccess(this, success);
    }

    @Override
    protected void onRegistrationFailure(Throwable failure) {
        log.info("Failed to register at shuffle manager {}.", getTargetAddress(), failure);

        registrationListener.onRegistrationFailure(failure);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private class RpcTargetRegistration extends RetryingRegistration<UUID, G, S> {

        private final Function<G, CompletableFuture<RegistrationResponse>> registrationFunction;

        public RpcTargetRegistration(
                Logger log,
                RemoteShuffleRpcService rpcService,
                String targetName,
                Class<G> targetType,
                String targetAddress,
                UUID fencingToken,
                RetryingRegistrationConfiguration retryingRegistrationConfiguration,
                Function<G, CompletableFuture<RegistrationResponse>> registrationFunction) {

            super(
                    log,
                    rpcService,
                    targetName,
                    targetType,
                    targetAddress,
                    fencingToken,
                    retryingRegistrationConfiguration);
            this.registrationFunction = registrationFunction;
        }

        @Override
        protected CompletableFuture<RegistrationResponse> invokeRegistration(
                G gateway, UUID fencingToken) throws Exception {
            return registrationFunction.apply(gateway).thenApply(result -> result);
        }
    }
}
