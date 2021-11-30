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

package com.alibaba.flink.shuffle.yarn.entry.manager;

import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManagerRunner;
import com.alibaba.flink.shuffle.yarn.utils.YarnConstants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Yarn deployment entry point of {@link ShuffleManagerRunner}. Reference doc :
 * https://hadoop.apache.org/docs/r2.7.1/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
 *
 * <p>Starting Shuffle Manager by submitting a Yarn application, and it will run in the application
 * master container. Manager related configurations in {@link YarnConstants} should be specified
 * when submitting the application.
 */
public class YarnShuffleManagerEntrypoint {
    private static final Logger LOG = LoggerFactory.getLogger(YarnShuffleManagerEntrypoint.class);

    private static boolean runAppClient(String[] args) throws Exception {
        AppClient client = new AppClient(args);
        return client.run();
    }

    public static void main(String[] args) {
        boolean success = false;
        try {
            success = runAppClient(args);
        } catch (Throwable t) {
            LOG.error("Starting Shuffle Manager application encountered an error, ", t);
            return;
        }
        if (success) {
            LOG.info("Start Shuffle Manager application successfully");
            return;
        }
        LOG.error("Start Shuffle Manager on Yarn failed");
    }
}
