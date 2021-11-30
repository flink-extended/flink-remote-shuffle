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

package com.alibaba.flink.shuffle.kubernetes.operator.parameters;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.core.config.ClusterOptions;
import com.alibaba.flink.shuffle.core.config.KubernetesOptions;
import com.alibaba.flink.shuffle.kubernetes.operator.parameters.util.ConfigMapVolume;
import com.alibaba.flink.shuffle.kubernetes.operator.util.Constants;
import com.alibaba.flink.shuffle.kubernetes.operator.util.KubernetesInternalOptions;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkArgument;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;

/** Abstract class for the {@link KubernetesPodParameters}. */
public abstract class AbstractKubernetesParameters
        implements KubernetesContainerParameters, KubernetesPodParameters {

    protected final Configuration conf;

    public AbstractKubernetesParameters(Configuration conf) {
        this.conf = checkNotNull(conf);
    }

    @Override
    public String getNamespace() {
        final String namespace = conf.getString(KubernetesInternalOptions.NAMESPACE);
        checkArgument(
                !namespace.trim().isEmpty(),
                "Invalid " + KubernetesInternalOptions.NAMESPACE + ".");

        return namespace;
    }

    @Override
    public boolean enablePodHostNetwork() {
        return conf.getBoolean(KubernetesOptions.POD_HOST_NETWORK_ENABLED);
    }

    @Override
    public String getContainerImage() {
        final String containerImage = conf.getString(KubernetesOptions.CONTAINER_IMAGE);
        checkArgument(
                !containerImage.trim().isEmpty(),
                "Invalid " + KubernetesOptions.CONTAINER_IMAGE + ".");
        return containerImage;
    }

    @Override
    public String getContainerImagePullPolicy() {
        return conf.getString(KubernetesOptions.CONTAINER_IMAGE_PULL_POLICY);
    }

    @Override
    public List<ConfigMapVolume> getConfigMapVolumes() {
        Optional<String> volumeName =
                Optional.ofNullable(conf.getString(KubernetesInternalOptions.CONFIG_VOLUME_NAME));
        Optional<String> configMapName =
                Optional.ofNullable(
                        conf.getString(KubernetesInternalOptions.CONFIG_VOLUME_CONFIG_MAP_NAME));
        Optional<String> mountPath =
                Optional.ofNullable(
                        conf.getString(KubernetesInternalOptions.CONFIG_VOLUME_MOUNT_PATH));
        Map<String, String> items = conf.getMap(KubernetesInternalOptions.CONFIG_VOLUME_ITEMS);
        if (volumeName.isPresent()) {
            checkState(configMapName.isPresent());
            checkState(mountPath.isPresent());
            return Collections.singletonList(
                    new ConfigMapVolume(
                            volumeName.get(),
                            configMapName.get(),
                            checkNotNull(items),
                            mountPath.get()));
        } else {
            checkState(items.isEmpty());
            return Collections.emptyList();
        }
    }

    @Override
    public KubernetesContainerParameters getContainerParameters() {
        return this;
    }

    public String getClusterId() {
        final String clusterId = conf.getString(ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID);

        if (StringUtils.isBlank(clusterId)) {
            throw new IllegalArgumentException(
                    ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.key() + " must not be blank.");
        } else if (clusterId.length() > Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID) {
            throw new IllegalArgumentException(
                    ClusterOptions.REMOTE_SHUFFLE_CLUSTER_ID.key()
                            + " must be no more than "
                            + Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID
                            + " characters.");
        }

        return clusterId;
    }
}
