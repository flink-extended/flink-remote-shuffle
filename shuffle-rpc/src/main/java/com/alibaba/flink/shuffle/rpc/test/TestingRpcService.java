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

package com.alibaba.flink.shuffle.rpc.test;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.FutureUtils;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleFencedRpcGateway;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcGateway;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcServiceImpl;
import com.alibaba.flink.shuffle.rpc.utils.AkkaRpcServiceUtils;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/**
 * An RPC Service implementation for testing. This RPC service acts as a replacement for the regular
 * RPC service for cases where tests need to return prepared mock gateways instead of proper RPC
 * gateways.
 *
 * <p>The TestingRpcService can be used for example in the following fashion, using <i>Mockito</i>
 * for mocks and verification:
 *
 * <pre>{@code
 * TestingRpcService rpc = new TestingRpcService();
 *
 * ShuffleManagerGateway testGateway = mock(ShuffleManagerGateway.class);
 * rpc.registerGateway("myAddress", testGateway);
 *
 * MyComponentToTest component = new MyComponentToTest();
 * component.triggerSomethingThatCallsTheGateway();
 *
 * verify(testGateway, timeout(1000)).theTestMethod(any(UUID.class), anyString());
 * }</pre>
 */
public class TestingRpcService extends RemoteShuffleRpcServiceImpl {

    private static final Function<
                    RemoteShuffleRpcGateway, CompletableFuture<RemoteShuffleRpcGateway>>
            DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION = CompletableFuture::completedFuture;

    /** Map of pre-registered connections. */
    private final ConcurrentHashMap<String, RemoteShuffleRpcGateway> registeredConnections;

    private volatile Function<RemoteShuffleRpcGateway, CompletableFuture<RemoteShuffleRpcGateway>>
            rpcGatewayFutureFunction = DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION;

    /** Creates a new {@code TestingRpcService}, using the given configuration. */
    public TestingRpcService() {
        super(startRpcService());

        this.registeredConnections = new ConcurrentHashMap<>();
    }

    private static RemoteShuffleRpcService startRpcService() {
        try {
            return AkkaRpcServiceUtils.createRemoteRpcService(
                    new Configuration(), null, "0", null, Optional.empty());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Void> stopService() {
        final CompletableFuture<Void> terminationFuture = super.stopService();

        terminationFuture.whenComplete(
                (Void ignored, Throwable throwable) -> registeredConnections.clear());

        return terminationFuture;
    }

    // ------------------------------------------------------------------------
    // connections
    // ------------------------------------------------------------------------

    public void registerGateway(String address, RemoteShuffleRpcGateway gateway) {
        checkNotNull(address);
        checkNotNull(gateway);

        if (registeredConnections.putIfAbsent(address, gateway) != null) {
            throw new IllegalStateException("a gateway is already registered under " + address);
        }
    }

    @SuppressWarnings("unchecked")
    private <C extends RemoteShuffleRpcGateway> CompletableFuture<C> getRpcGatewayFuture(
            C gateway) {
        return (CompletableFuture<C>) rpcGatewayFutureFunction.apply(gateway);
    }

    public void clearGateways() {
        registeredConnections.clear();
    }

    public void resetRpcGatewayFutureFunction() {
        rpcGatewayFutureFunction = DEFAULT_RPC_GATEWAY_FUTURE_FUNCTION;
    }

    public void setRpcGatewayFutureFunction(
            Function<RemoteShuffleRpcGateway, CompletableFuture<RemoteShuffleRpcGateway>>
                    rpcGatewayFutureFunction) {
        this.rpcGatewayFutureFunction = rpcGatewayFutureFunction;
    }

    @Override
    public Executor getExecutor() {
        return getScheduledExecutor();
    }

    @Override
    public <F extends Serializable, C extends RemoteShuffleFencedRpcGateway<F>>
            CompletableFuture<C> connectTo(String address, F fencingToken, Class<C> clazz) {
        RemoteShuffleRpcGateway gateway = registeredConnections.get(address);

        if (gateway != null) {
            if (clazz.isAssignableFrom(gateway.getClass())) {
                @SuppressWarnings("unchecked")
                C typedGateway = (C) gateway;
                return getRpcGatewayFuture(typedGateway);
            } else {
                return FutureUtils.completedExceptionally(
                        new Exception(
                                "Gateway registered under "
                                        + address
                                        + " is not of type "
                                        + clazz));
            }
        } else {
            return super.connectTo(address, fencingToken, clazz);
        }
    }

    @Override
    public <C extends RemoteShuffleRpcGateway> CompletableFuture<C> connectTo(
            String address, Class<C> clazz) {
        RemoteShuffleRpcGateway gateway = registeredConnections.get(address);

        if (gateway != null) {
            if (clazz.isAssignableFrom(gateway.getClass())) {
                @SuppressWarnings("unchecked")
                C typedGateway = (C) gateway;
                return getRpcGatewayFuture(typedGateway);
            } else {
                return FutureUtils.completedExceptionally(
                        new Exception(
                                "Gateway registered under "
                                        + address
                                        + " is not of type "
                                        + clazz));
            }
        } else {
            return super.connectTo(address, clazz);
        }
    }
}
