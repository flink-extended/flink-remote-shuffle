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

package com.alibaba.flink.shuffle.coordinator.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.FatalErrorExitUtils;
import com.alibaba.flink.shuffle.core.utils.ConfigurationParserUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for running manager and workers. */
public final class ClusterEntrypointUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypointUtils.class);

    public static final int STARTUP_FAILURE_RETURN_CODE = 1;

    private ClusterEntrypointUtils() {
        throw new UnsupportedOperationException("This class should not be instantiated.");
    }

    /**
     * Parses passed String array using the parameter definitions of the passed {@code
     * ParserResultFactory}. The method will call {@code System.exit} and print the usage
     * information to stdout in case of a parsing error.
     *
     * @param args The String array that shall be parsed.
     * @return The parsing result.
     */
    public static Configuration parseParametersOrExit(String[] args) {

        try {
            return ConfigurationParserUtils.loadConfiguration(args);
        } catch (Exception e) {
            LOG.error("Could not parse command line arguments {}.", args, e);
            FatalErrorExitUtils.exitProcessIfNeeded(STARTUP_FAILURE_RETURN_CODE);
        }

        return null;
    }
}
