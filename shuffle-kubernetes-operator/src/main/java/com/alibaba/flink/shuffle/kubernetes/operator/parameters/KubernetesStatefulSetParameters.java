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

import io.fabric8.kubernetes.api.model.Quantity;

import java.util.List;
import java.util.Map;

/** The parameters for StatefulSet. */
public interface KubernetesStatefulSetParameters extends KubernetesCommonParameters {

    String getStatefulSetName();

    KubernetesPodParameters getPodTemplateParameters();

    int getReplicas();

    Map<String, Quantity> getStorageResource();

    String getStorageClass();

    List<String> getAccessMode();

    String getMountPath();
}
