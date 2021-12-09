/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.common.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A configuration that manages a subset of keys with a common prefix from a given configuration.
 */
public final class DelegatingConfiguration extends Configuration {

    private static final long serialVersionUID = 1L;

    private final Configuration backingConfig; // the configuration actually storing the data

    private String prefix; // the prefix key by which keys for this config are marked

    // --------------------------------------------------------------------------------------------

    /** Default constructor for serialization. Creates an empty delegating configuration. */
    public DelegatingConfiguration() {
        this.backingConfig = new Configuration();
        this.prefix = "";
    }

    /**
     * Creates a new delegating configuration which stores its key/value pairs in the given
     * configuration using the specifies key prefix.
     *
     * @param backingConfig The configuration holding the actual config data.
     * @param prefix The prefix prepended to all config keys.
     */
    public DelegatingConfiguration(Configuration backingConfig, String prefix) {
        this.backingConfig = backingConfig;
        this.prefix = prefix;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String getString(String key, String defaultValue) {
        return this.backingConfig.getString(this.prefix + key, defaultValue);
    }

    @Override
    public String getString(ConfigOption<String> configOption) {
        return this.backingConfig.getString(prefixOption(configOption, prefix));
    }

    @Override
    public String getString(ConfigOption<String> configOption, String overrideDefault) {
        return this.backingConfig.getString(prefixOption(configOption, prefix), overrideDefault);
    }

    @Override
    public void setString(String key, String value) {
        this.backingConfig.setString(this.prefix + key, value);
    }

    @Override
    public void setString(ConfigOption<String> key, String value) {
        this.backingConfig.setString(prefix + key.key(), value);
    }

    @Override
    public Integer getInteger(ConfigOption<Integer> configOption) {
        return this.backingConfig.getInteger(prefixOption(configOption, prefix));
    }

    @Override
    public Long getLong(ConfigOption<Long> configOption) {
        return this.backingConfig.getLong(prefixOption(configOption, prefix));
    }

    @Override
    public Boolean getBoolean(ConfigOption<Boolean> configOption) {
        return this.backingConfig.getBoolean(prefixOption(configOption, prefix));
    }

    @Override
    public Float getFloat(ConfigOption<Float> configOption) {
        return this.backingConfig.getFloat(prefixOption(configOption, prefix));
    }

    @Override
    public Double getDouble(ConfigOption<Double> configOption) {
        return this.backingConfig.getDouble(prefixOption(configOption, prefix));
    }

    @Override
    public String toString() {
        return backingConfig.toString();
    }

    @Override
    public Map<String, String> toMap() {
        Map<String, String> map = backingConfig.toMap();
        Map<String, String> prefixed = new HashMap<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (entry.getKey().startsWith(prefix)) {
                String keyWithoutPrefix = entry.getKey().substring(prefix.length());
                prefixed.put(keyWithoutPrefix, entry.getValue());
            }
        }
        return prefixed;
    }

    @Override
    public Properties toProperties() {
        Properties clonedConfiguration = new Properties();
        clonedConfiguration.putAll(toMap());
        return clonedConfiguration;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return this.prefix.hashCode() ^ this.backingConfig.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof DelegatingConfiguration) {
            DelegatingConfiguration other = (DelegatingConfiguration) obj;
            return this.prefix.equals(other.prefix)
                    && this.backingConfig.equals(other.backingConfig);
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------

    private static <T> ConfigOption<T> prefixOption(ConfigOption<T> option, String prefix) {
        String key = prefix + option.key();
        return new ConfigOption<T>(key);
    }
}
