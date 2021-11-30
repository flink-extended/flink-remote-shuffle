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

package com.alibaba.flink.shuffle.yarn.utils;

import com.alibaba.flink.shuffle.common.config.ConfigOption;

/** This class holds configuration constants used by the remote shuffle on Yarn deployment. */
public class YarnOptions {

    /**
     * Flag indicating whether to throw the encountered exceptions to the upper Yarn service. The
     * parameter's default value is false. If it is set as true, the upper Yarn service may be
     * stopped because of the exceptions from the ShuffleWorker. Note: This parameter needs to be
     * configured in yarn-site.xml.
     */
    public static final ConfigOption<Boolean> WORKER_STOP_ON_FAILURE =
            new ConfigOption<Boolean>("remote-shuffle.yarn.worker-stop-on-failure")
                    .defaultValue(false)
                    .description(
                            "Flag indicating whether to throw the encountered exceptions to the "
                                    + "upper Yarn service. The parameter's default value is false. "
                                    + "If it is set as true, the upper Yarn service may be stopped "
                                    + "because of the exceptions from the ShuffleWorker. Note: This"
                                    + " parameter needs to be configured in yarn-site.xml.");
}
