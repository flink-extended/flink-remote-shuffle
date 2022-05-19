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

package com.alibaba.flink.shuffle.coordinator.worker;

import java.io.Serializable;
import java.util.HashMap;

/** Shuffle worker metrics. */
public class ShuffleWorkerMetrics implements Serializable {

    private static final long serialVersionUID = -1460652050573532972L;

    private final HashMap<String, Serializable> metrics = new HashMap<>();

    public void setMetric(String key, Serializable metric) {
        metrics.put(key, metric);
    }

    public Serializable getMetric(String key) {
        return metrics.get(key);
    }

    public Integer getIntegerMetric(String key) {
        return (Integer) getMetric(key);
    }

    public Long getLongMetric(String key) {
        return (Long) getMetric(key);
    }

    public Double getDoubleMetric(String key) {
        return (Double) getMetric(key);
    }

    public Float getFloatMetric(String key) {
        return (Float) getMetric(key);
    }

    public String getStringMetric(String key) {
        return (String) getMetric(key);
    }

    @Override
    public String toString() {
        return metrics.toString();
    }
}
