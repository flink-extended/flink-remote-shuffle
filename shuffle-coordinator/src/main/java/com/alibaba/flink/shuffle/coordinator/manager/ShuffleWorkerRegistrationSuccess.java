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

package com.alibaba.flink.shuffle.coordinator.manager;

import com.alibaba.flink.shuffle.core.ids.InstanceID;
import com.alibaba.flink.shuffle.core.ids.RegistrationID;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Indicates a successful response from ShuffleManager to ShuffleWorker when registration. */
public final class ShuffleWorkerRegistrationSuccess extends RegistrationSuccess {

    private static final long serialVersionUID = -3022884771553508472L;

    /** The registration id of the shuffle worker. */
    private final RegistrationID registrationID;

    /**
     * Create a new {@code ShuffleWorkerToManagerRegistrationSuccess} message.
     *
     * @param registrationID The ID that the ShuffleManager assigned the registration.
     * @param managerID The unique ID that identifies the ShuffleManager.
     */
    public ShuffleWorkerRegistrationSuccess(RegistrationID registrationID, InstanceID managerID) {

        super(managerID);
        this.registrationID = checkNotNull(registrationID);
    }

    /** Gets the ID that the ShuffleManager assigned the registration. */
    public RegistrationID getRegistrationID() {
        return registrationID;
    }

    /** Gets the unique ID that identifies the ShuffleManager. */
    @Override
    public String toString() {
        return "ShuffleWorkerToManagerRegistrationSuccess{"
                + "registrationId="
                + registrationID
                + ", shuffleManagerInstanceID="
                + getInstanceID()
                + '}';
    }
}
