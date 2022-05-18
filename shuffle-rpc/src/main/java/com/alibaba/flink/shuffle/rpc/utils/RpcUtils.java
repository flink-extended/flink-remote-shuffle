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

package com.alibaba.flink.shuffle.rpc.utils;

import com.alibaba.flink.shuffle.rpc.RemoteShuffleFencedRpcEndpoint;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcEndpoint;
import com.alibaba.flink.shuffle.rpc.RemoteShuffleRpcService;

import org.apache.flink.runtime.rpc.RpcEndpoint;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Util methods for remote shuffle rpc. */
public class RpcUtils {

    /**
     * Shuts the given {@link RpcEndpoint} down and awaits its termination.
     *
     * @param rpcEndpoint to terminate
     * @param timeout for this operation
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the operation has been interrupted
     * @throws TimeoutException if a timeout occurred
     */
    public static void terminateRpcEndpoint(RemoteShuffleRpcEndpoint rpcEndpoint, long timeout)
            throws ExecutionException, InterruptedException, TimeoutException {
        rpcEndpoint.closeAsync().get(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Shuts the given {@link RpcEndpoint} down and awaits its termination.
     *
     * @param rpcEndpoint to terminate
     * @param timeout for this operation
     * @throws ExecutionException if a problem occurred
     * @throws InterruptedException if the operation has been interrupted
     * @throws TimeoutException if a timeout occurred
     */
    public static void terminateRpcEndpoint(
            RemoteShuffleFencedRpcEndpoint<?> rpcEndpoint, long timeout)
            throws ExecutionException, InterruptedException, TimeoutException {
        rpcEndpoint.closeAsync().get(timeout, TimeUnit.MILLISECONDS);
    }

    /**
     * Shuts the given rpc service down and waits for its termination.
     *
     * @param rpcService to shut down
     * @param timeout for this operation
     * @throws InterruptedException if the operation has been interrupted
     * @throws ExecutionException if a problem occurred
     * @throws TimeoutException if a timeout occurred
     */
    public static void terminateRpcService(RemoteShuffleRpcService rpcService, long timeout)
            throws InterruptedException, ExecutionException, TimeoutException {
        rpcService.stopService().get(timeout, TimeUnit.MILLISECONDS);
    }
}
