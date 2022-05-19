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

package com.alibaba.flink.shuffle.rpc;

import com.alibaba.flink.shuffle.rpc.executor.RpcMainThreadScheduledExecutor;
import com.alibaba.flink.shuffle.rpc.executor.ScheduledExecutor;

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;

import javax.annotation.Nullable;

import java.io.Serializable;

/** {@link FencedRpcEndpoint} for remote shuffle. */
public abstract class RemoteShuffleFencedRpcEndpoint<F extends Serializable>
        extends FencedRpcEndpoint<F> {

    protected RemoteShuffleFencedRpcEndpoint(
            RemoteShuffleRpcService rpcService, String endpointId, @Nullable F fencingToken) {
        super(rpcService, endpointId, fencingToken);
    }

    @Override
    public RemoteShuffleRpcService getRpcService() {
        return (RemoteShuffleRpcService) super.getRpcService();
    }

    public ScheduledExecutor getRpcMainThreadScheduledExecutor() {
        return new RpcMainThreadScheduledExecutor(getMainThreadExecutor());
    }
}
