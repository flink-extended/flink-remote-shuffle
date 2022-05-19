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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;

/**
 * High availability mode for remote shuffle cluster execution. Currently, supported modes are:
 *
 * <p>- NONE: No high availability. - ZooKeeper: ShuffleManager high availability via ZooKeeper is
 * used to select a leader among a group of ShuffleManager. This ShuffleManager is responsible for
 * managing the ShuffleWorker. Upon failure of the leader a new leader is elected which will take
 * over the responsibilities of the old leader. - FACTORY_CLASS: Use implementation * of {@link
 * HaServicesFactory} specified in configuration property high-availability
 */
public enum HaMode {
    NONE(false),
    ZOOKEEPER(true),
    FACTORY_CLASS(true);

    private final boolean haActive;

    HaMode(boolean haActive) {
        this.haActive = haActive;
    }

    public static HaMode fromConfig(Configuration config) {
        String haMode = config.getString(HighAvailabilityOptions.HA_MODE);

        if (haMode == null) {
            return HaMode.NONE;
        } else if (haMode.equalsIgnoreCase("NONE")) {
            // Map old default to new default
            return HaMode.NONE;
        } else {
            try {
                return HaMode.valueOf(haMode.toUpperCase());
            } catch (IllegalArgumentException e) {
                return FACTORY_CLASS;
            }
        }
    }

    /**
     * Returns true if the defined recovery mode supports high availability.
     *
     * @param configuration Configuration which contains the recovery mode
     * @return true if high availability is supported by the recovery mode, otherwise false
     */
    public static boolean isHighAvailabilityModeActivated(Configuration configuration) {
        HaMode mode = fromConfig(configuration);
        return mode.haActive;
    }
}
