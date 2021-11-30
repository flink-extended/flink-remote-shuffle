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

package com.alibaba.flink.shuffle.e2e.flinkcluster;

import com.alibaba.flink.shuffle.e2e.TestJvmProcess;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint;
import org.apache.flink.runtime.jobmanager.JobManagerProcessSpec;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

/** A {@link Dispatcher} instance running in a separate JVM. */
public class DispatcherProcess extends TestJvmProcess {

    private static final Logger LOG = LoggerFactory.getLogger(DispatcherProcess.class);

    private final String[] jvmArgs;

    public DispatcherProcess(String logDirName, Configuration config) throws Exception {
        super("Dispatcher", logDirName);
        ArrayList<String> args = new ArrayList<>();

        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            args.add("--" + entry.getKey());
            args.add(entry.getValue());
        }

        this.jvmArgs = new String[args.size()];
        args.toArray(jvmArgs);

        final JobManagerProcessSpec processSpec =
                JobManagerProcessUtils.processSpecFromConfigWithNewOptionToInterpretLegacyHeap(
                        config, JobManagerOptions.JVM_HEAP_MEMORY);

        setJVMHeapMemory(processSpec.getJvmHeapMemorySize().getMebiBytes());
        setJvmDirectMemory(processSpec.getJvmDirectMemorySize().getMebiBytes());
        LOG.info(
                "JVM Process {} with process memory spec {}, heap memory {}, direct memory {}",
                getName(),
                processSpec,
                processSpec.getJvmHeapMemorySize().getMebiBytes(),
                processSpec.getJvmDirectMemorySize().getMebiBytes());
    }

    @Override
    public String[] getJvmArgs() {
        return jvmArgs;
    }

    @Override
    public String getEntryPointClassName() {
        return EntryPoint.class.getName();
    }

    /** Entry point for the Dispatcher process. */
    public static class EntryPoint {

        private static final Logger LOG = LoggerFactory.getLogger(EntryPoint.class);

        /**
         * Entrypoint of the DispatcherProcessEntryPoint.
         *
         * <p>Other arguments are parsed to a {@link Configuration} and passed to the Dispatcher,
         * for instance: <code>--high-availability ZOOKEEPER --high-availability.zookeeper.quorum
         * "xyz:123:456"</code>.
         */
        public static void main(String[] args) {
            try {
                ParameterTool params = ParameterTool.fromArgs(args);
                Configuration config = params.getConfiguration();
                LOG.info("Configuration: {}.", config);

                config.setInteger(JobManagerOptions.PORT, 0);
                config.setString(RestOptions.BIND_PORT, "0");

                final StandaloneSessionClusterEntrypoint clusterEntrypoint =
                        new StandaloneSessionClusterEntrypoint(config);

                ClusterEntrypoint.runClusterEntrypoint(clusterEntrypoint);

            } catch (Throwable t) {
                t.printStackTrace();
                LOG.error("Failed to start Dispatcher process", t);
                System.exit(1);
            }
        }
    }
}
