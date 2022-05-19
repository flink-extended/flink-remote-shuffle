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

package com.alibaba.flink.shuffle.kubernetes.operator.crd;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/** Status of the {@link RemoteShuffleApplication}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
public class RemoteShuffleApplicationStatus {

    private int readyShuffleManagers;
    private int readyShuffleWorkers;
    private int desiredShuffleManagers;
    private int desiredShuffleWorkers;

    public RemoteShuffleApplicationStatus() {}

    RemoteShuffleApplicationStatus(
            int readyShuffleManagers,
            int readyShuffleWorkers,
            int desiredShuffleManagers,
            int desiredShuffleWorkers) {

        this.readyShuffleManagers = readyShuffleManagers;
        this.readyShuffleWorkers = readyShuffleWorkers;
        this.desiredShuffleManagers = desiredShuffleManagers;
        this.desiredShuffleWorkers = desiredShuffleWorkers;
    }

    public int getReadyShuffleManagers() {
        return readyShuffleManagers;
    }

    public void setReadyShuffleManagers(int readyShuffleManagers) {
        this.readyShuffleManagers = readyShuffleManagers;
    }

    public int getReadyShuffleWorkers() {
        return readyShuffleWorkers;
    }

    public void setReadyShuffleWorkers(int readyShuffleWorkers) {
        this.readyShuffleWorkers = readyShuffleWorkers;
    }

    public int getDesiredShuffleManagers() {
        return desiredShuffleManagers;
    }

    public void setDesiredShuffleManagers(int desiredShuffleManagers) {
        this.desiredShuffleManagers = desiredShuffleManagers;
    }

    public int getDesiredShuffleWorkers() {
        return desiredShuffleWorkers;
    }

    public void setDesiredShuffleWorkers(int desiredShuffleWorkers) {
        this.desiredShuffleWorkers = desiredShuffleWorkers;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RemoteShuffleApplicationStatus) {
            RemoteShuffleApplicationStatus status = (RemoteShuffleApplicationStatus) obj;
            return this.readyShuffleManagers == status.readyShuffleManagers
                    && this.readyShuffleWorkers == status.readyShuffleWorkers
                    && this.desiredShuffleManagers == status.desiredShuffleManagers
                    && this.desiredShuffleWorkers == status.desiredShuffleWorkers;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "RemoteShuffleApplicationStatus("
                + "readyShuffleManagers="
                + readyShuffleManagers
                + ", readyShuffleWorkers="
                + readyShuffleWorkers
                + ", desiredShuffleManagers="
                + desiredShuffleManagers
                + ", desiredShuffleWorkers="
                + desiredShuffleWorkers
                + ")";
    }
}
