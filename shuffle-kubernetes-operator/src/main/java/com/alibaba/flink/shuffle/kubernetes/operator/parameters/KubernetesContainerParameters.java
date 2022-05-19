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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ContainerCommandAndArgs;

import java.util.List;
import java.util.Map;

/** The parameters that is used to construct a kubernetes Container. */
public interface KubernetesContainerParameters {

    String getContainerName();

    String getContainerImage();

    String getContainerImagePullPolicy();

    List<Map<String, String>> getContainerVolumeMounts();

    Integer getContainerMemoryMB();

    Double getContainerCPU();

    Map<String, String> getResourceLimitFactors();

    ContainerCommandAndArgs getContainerCommandAndArgs();

    Map<String, String> getEnvironmentVars();
}
