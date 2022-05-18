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

package com.alibaba.flink.shuffle.kubernetes.operator.crd;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Kind;
import io.fabric8.kubernetes.model.annotation.Plural;
import io.fabric8.kubernetes.model.annotation.Singular;
import io.fabric8.kubernetes.model.annotation.Version;

/**
 * {@link RemoteShuffleApplication} is an implementation of Kubernetes {@link CustomResource}, which
 * represents a remote shuffle cluster. {@link #spec} contains the configurations of the cluster,
 * and {@link #status} represents current status of the cluster.
 */
@Version("v1")
@Group("shuffleoperator.alibaba.com")
@Kind(RemoteShuffleApplication.KIND)
@Singular("remoteshuffle")
@Plural("remoteshuffles")
public class RemoteShuffleApplication
        extends CustomResource<RemoteShuffleApplicationSpec, RemoteShuffleApplicationStatus>
        implements Namespaced {

    private static final long serialVersionUID = -7093257940691211895L;

    public static final String KIND = "RemoteShuffle";
}
