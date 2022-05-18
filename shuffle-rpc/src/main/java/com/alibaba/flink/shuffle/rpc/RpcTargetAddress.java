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

import java.util.Objects;
import java.util.UUID;

/** The address of a rpc service with registration. */
public class RpcTargetAddress {

    /** The rpc address. */
    private final String targetAddress;

    /** The uuid for the target. */
    private final UUID leaderUUID;

    public RpcTargetAddress(String targetAddress, UUID leaderUUID) {
        this.targetAddress = targetAddress;
        this.leaderUUID = leaderUUID;
    }

    public String getTargetAddress() {
        return targetAddress;
    }

    public UUID getLeaderUUID() {
        return leaderUUID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RpcTargetAddress that = (RpcTargetAddress) o;
        return Objects.equals(targetAddress, that.targetAddress)
                && Objects.equals(leaderUUID, that.leaderUUID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetAddress, leaderUUID);
    }

    @Override
    public String toString() {
        return "RpcRegistrationConnection{"
                + "targetAddress='"
                + targetAddress
                + "', leaderUUID="
                + leaderUUID
                + '}';
    }
}
