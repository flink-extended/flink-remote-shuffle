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

package com.alibaba.flink.shuffle.rpc;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;

import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A {@link RemoteShuffleRpcService} implementation which simply delegates to another {@link
 * RpcService} instance.
 */
public class RemoteShuffleRpcServiceImpl implements RemoteShuffleRpcService {

    private final RpcService rpcService;

    public RemoteShuffleRpcServiceImpl(RpcService rpcService) {
        CommonUtils.checkArgument(rpcService != null, "Must be not null.");
        this.rpcService = rpcService;
    }

    @Override
    public String getAddress() {
        return rpcService.getAddress();
    }

    @Override
    public int getPort() {
        return rpcService.getPort();
    }

    @Override
    public <C extends RpcGateway> CompletableFuture<C> connect(String address, Class<C> clazz) {
        return rpcService.connect(address, clazz);
    }

    @Override
    public <F extends Serializable, C extends FencedRpcGateway<F>> CompletableFuture<C> connect(
            String address, F fencingToken, Class<C> clazz) {
        return rpcService.connect(address, fencingToken, clazz);
    }

    @Override
    public <C extends RpcEndpoint & RpcGateway> RpcServer startServer(C rpcEndpoint) {
        return rpcService.startServer(rpcEndpoint);
    }

    @Override
    public <F extends Serializable> RpcServer fenceRpcServer(RpcServer rpcServer, F fencingToken) {
        return rpcService.fenceRpcServer(rpcServer, fencingToken);
    }

    @Override
    public void stopServer(RpcServer selfGateway) {
        rpcService.stopServer(selfGateway);
    }

    @Override
    public CompletableFuture<Void> stopService() {
        return rpcService.stopService();
    }

    @Override
    public CompletableFuture<Void> getTerminationFuture() {
        return rpcService.getTerminationFuture();
    }

    @Override
    public ScheduledExecutor getScheduledExecutor() {
        return rpcService.getScheduledExecutor();
    }

    @Override
    public ScheduledFuture<?> scheduleRunnable(Runnable runnable, long delay, TimeUnit unit) {
        return rpcService.scheduleRunnable(runnable, delay, unit);
    }

    @Override
    public void execute(Runnable runnable) {
        rpcService.execute(runnable);
    }

    @Override
    public <T> CompletableFuture<T> execute(Callable<T> callable) {
        return rpcService.execute(callable);
    }

    @Override
    public Executor getExecutor() {
        return getScheduledExecutor();
    }

    @Override
    public <F extends Serializable, C extends RemoteShuffleFencedRpcGateway<F>>
            CompletableFuture<C> connectTo(String address, F fencingToken, Class<C> clazz) {
        return connect(address, fencingToken, clazz);
    }

    @Override
    public <C extends RemoteShuffleRpcGateway> CompletableFuture<C> connectTo(
            String address, Class<C> clazz) {
        return connect(address, clazz);
    }
}
