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

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Specifications for {@link RemoteShuffleApplication}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
public class RemoteShuffleApplicationSpec {

    private Map<String, String> shuffleDynamicConfigs;

    @Nullable private Map<String, String> shuffleFileConfigs;

    public Map<String, String> getShuffleDynamicConfigs() {
        checkNotNull(shuffleDynamicConfigs);
        return shuffleDynamicConfigs;
    }

    public RemoteShuffleApplicationSpec() {}

    public RemoteShuffleApplicationSpec(
            Map<String, String> shuffleDynamicConfigs,
            @Nullable Map<String, String> shuffleFileConfigs) {
        this.shuffleFileConfigs = shuffleFileConfigs;
        this.shuffleDynamicConfigs = checkNotNull(shuffleDynamicConfigs);
    }

    public void setShuffleDynamicConfigs(Map<String, String> shuffleDynamicConfigs) {
        this.shuffleDynamicConfigs = checkNotNull(shuffleDynamicConfigs);
    }

    @Nullable
    public Map<String, String> getShuffleFileConfigs() {
        return shuffleFileConfigs;
    }

    public void setShuffleFileConfigs(@Nullable Map<String, String> shuffleFileConfigs) {
        this.shuffleFileConfigs = shuffleFileConfigs;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof RemoteShuffleApplicationSpec) {
            RemoteShuffleApplicationSpec spec = (RemoteShuffleApplicationSpec) obj;
            return shuffleDynamicConfigs.equals(spec.shuffleDynamicConfigs)
                    && Objects.equals(shuffleFileConfigs, spec.shuffleFileConfigs);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "RemoteShuffleApplicationSpec("
                + "shuffleDynamicConfigs="
                + shuffleDynamicConfigs
                + ", shuffleFileConfigs="
                + shuffleFileConfigs
                + ")";
    }
}
