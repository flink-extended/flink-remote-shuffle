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

package com.alibaba.flink.shuffle.coordinator.highavailability;

import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.ProtocolUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * Information about leader including the confirmed leader session id and leader address.
 *
 * <p>This class is copied from Apache Flink
 * (org.apache.flink.runtime.leaderelection.LeaderInformation).
 */
public class LeaderInformation implements Serializable {

    private static final long serialVersionUID = -5345049443329244907L;

    private static final String IS_EMPTY_KEY = "isEmpty";

    private static final String PROTOCOL_VERSION_KEY = "protocolVersion";

    private static final String SUPPORTED_VERSION_KEY = "supportedVersion";

    private static final String LEADER_ID_KEY = "leaderSessionID";

    private static final String LEADER_ADDRESS_KEY = "leaderAddress";

    private final int protocolVersion;

    private final int supportedVersion;

    private final UUID leaderSessionID;

    private final String leaderAddress;

    private static final LeaderInformation EMPTY =
            new LeaderInformation(HaServices.DEFAULT_LEADER_ID, "");

    public LeaderInformation(UUID leaderSessionID, String leaderAddress) {
        this(
                ProtocolUtils.currentProtocolVersion(),
                ProtocolUtils.compatibleVersion(),
                leaderSessionID,
                leaderAddress);
    }

    public LeaderInformation(
            int protocolVersion, int supportedVersion, UUID leaderSessionID, String leaderAddress) {
        CommonUtils.checkArgument(leaderSessionID != null, "Must be not null.");
        CommonUtils.checkArgument(leaderAddress != null, "Must be not null.");

        this.protocolVersion = protocolVersion;
        this.supportedVersion = supportedVersion;
        this.leaderSessionID = leaderSessionID;
        this.leaderAddress = leaderAddress;
    }

    public int getProtocolVersion() {
        return protocolVersion;
    }

    public int getSupportedVersion() {
        return supportedVersion;
    }

    public UUID getLeaderSessionID() {
        return leaderSessionID;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }

    public boolean isEmpty() {
        return this == EMPTY;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof LeaderInformation)) {
            return false;
        }

        LeaderInformation that = (LeaderInformation) obj;
        return Objects.equals(this.leaderSessionID, that.leaderSessionID)
                && Objects.equals(this.leaderAddress, that.leaderAddress);
    }

    @Override
    public int hashCode() {
        return Objects.hash(leaderSessionID, leaderAddress);
    }

    public static LeaderInformation empty() {
        return EMPTY;
    }

    public static LeaderInformation fromByteArray(byte[] bytes) throws Exception {
        try (ByteArrayInputStream input = new ByteArrayInputStream(bytes)) {
            Properties properties = new Properties();
            properties.load(input);

            boolean isEmpty = Boolean.parseBoolean(properties.getProperty(IS_EMPTY_KEY));
            if (isEmpty) {
                return EMPTY;
            }

            return new LeaderInformation(
                    Integer.parseInt(properties.getProperty(PROTOCOL_VERSION_KEY)),
                    Integer.parseInt(properties.getProperty(SUPPORTED_VERSION_KEY)),
                    UUID.fromString(properties.getProperty(LEADER_ID_KEY)),
                    properties.getProperty(LEADER_ADDRESS_KEY));
        }
    }

    public byte[] toByteArray() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(IS_EMPTY_KEY, String.valueOf(isEmpty()));
        properties.setProperty(PROTOCOL_VERSION_KEY, String.valueOf(protocolVersion));
        properties.setProperty(SUPPORTED_VERSION_KEY, String.valueOf(supportedVersion));
        properties.setProperty(LEADER_ID_KEY, leaderSessionID.toString());
        properties.setProperty(LEADER_ADDRESS_KEY, leaderAddress);

        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            properties.store(output, null);
            return output.toByteArray();
        }
    }

    @Override
    public String toString() {
        return String.format(
                "LeaderInformation{leaderSessionID=%s, leaderAddress=%s, isEmpty=%s, "
                        + "protocolVersion=%d, supportedVersion=%d}",
                leaderSessionID, leaderAddress, isEmpty(), protocolVersion, supportedVersion);
    }
}
