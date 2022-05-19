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

package com.alibaba.flink.shuffle.yarn.entry.manager;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.JvmShutdownSafeguard;
import com.alibaba.flink.shuffle.common.utils.SignalHandler;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManager;
import com.alibaba.flink.shuffle.coordinator.manager.entrypoint.ShuffleManagerEntrypoint;
import com.alibaba.flink.shuffle.coordinator.utils.EnvironmentInformation;
import com.alibaba.flink.shuffle.core.config.ManagerOptions;
import com.alibaba.flink.shuffle.yarn.utils.DeployOnYarnUtils;
import com.alibaba.flink.shuffle.yarn.utils.YarnConstants;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.alibaba.flink.shuffle.common.config.Configuration.REMOTE_SHUFFLE_CONF_FILENAME;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME;

/** This class is meant to start {@link ShuffleManager} based on a yarn-based application master. */
public class YarnShuffleManagerRunner {
    private static final Logger LOG = LoggerFactory.getLogger(YarnShuffleManagerRunner.class);

    private final String[] args;

    private final Configuration conf;

    private String amHost;

    private static final int AM_RPC_PORT_DEFAULT = -1;

    public YarnShuffleManagerRunner(String[] args) throws ParseException {
        this.args = args;
        this.conf = mergeOptionsWithConfigurationFile(args);
    }

    private Configuration mergeOptionsWithConfigurationFile(String[] args) throws ParseException {
        Configuration optionsInArgs = DeployOnYarnUtils.parseParameters(args);
        File confFile = new File(MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME, REMOTE_SHUFFLE_CONF_FILENAME);
        String confFilePath = confFile.getAbsolutePath();
        if (!confFile.exists()) {
            LOG.info("Configuration file " + confFilePath + " is not exist");
            return optionsInArgs;
        }

        Configuration mergedOptions = new Configuration();
        Configuration optionsInFile;
        try {
            optionsInFile = new Configuration(confFile.getParent());
        } catch (IOException ioe) {
            LOG.error("Failed to load options in the configuration file " + confFilePath, ioe);
            return optionsInArgs;
        }
        LOG.info(
                "Loaded all options in the configuration file "
                        + confFilePath
                        + ", options count: "
                        + optionsInFile.toMap().size());
        // The options in the input parameters will override the options in the configuration file
        mergedOptions.addAll(optionsInFile);
        mergedOptions.addAll(optionsInArgs);
        return mergedOptions;
    }

    /** Main run function for the application master. */
    public void run() throws Exception {
        setShuffleManagerAddress();
        startShuffleManagerInternal();
    }

    private void setShuffleManagerAddress() throws IOException, YarnException {
        amHost = getAMContainerHost();
        CommonUtils.checkArgument(
                amHost != null && !amHost.isEmpty(),
                "The Shuffle Manager address must not be empty.");
        conf.setString(ManagerOptions.RPC_ADDRESS, amHost);
        LOG.info(
                "Set Shuffle Manager address "
                        + ManagerOptions.RPC_ADDRESS.key()
                        + " as "
                        + amHost);
    }

    private String getAMContainerHost() throws IOException, YarnException {
        long timestamp = conf.getLong(YarnConstants.MANAGER_AM_APPID_TIMESTAMP_KEY, -1L);
        int id = conf.getInteger(YarnConstants.MANAGER_AM_APPID_ID_KEY, -1);
        if (timestamp < 0 || id < 0) {
            throw new IOException(
                    "Unable to resolve appid because id or timestamp is empty, id: "
                            + id
                            + " timestamp: "
                            + timestamp);
        }
        ApplicationId applicationId = ApplicationId.newInstance(timestamp, id);

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(new YarnConfiguration());
        yarnClient.start();

        // Get container report from Yarn
        List<ApplicationAttemptReport> attemptReports =
                yarnClient.getApplicationAttempts(applicationId);
        int reportIndex = findMaxAttemptIndex(attemptReports);
        ContainerReport containerReport =
                yarnClient.getContainerReport(attemptReports.get(reportIndex).getAMContainerId());

        // Dump AM container report
        LOG.info(
                "Dump AM container report. id: "
                        + containerReport.getContainerId()
                        + " state: "
                        + containerReport.getContainerState()
                        + " assignedNode: "
                        + containerReport.getAssignedNode().getHost()
                        + " url: "
                        + containerReport.getLogUrl());
        return containerReport.getAssignedNode().getHost();
    }

    private int findMaxAttemptIndex(List<ApplicationAttemptReport> attemptReports) {
        int maxAttempt = 0;
        for (ApplicationAttemptReport attemptReport : attemptReports) {
            int currentAttemptId = attemptReport.getApplicationAttemptId().getAttemptId();
            maxAttempt = Math.max(currentAttemptId, maxAttempt);
        }
        for (int idx = 0; idx < attemptReports.size(); idx++) {
            if (maxAttempt == attemptReports.get(idx).getApplicationAttemptId().getAttemptId()) {
                return idx;
            }
        }
        return attemptReports.size() - 1;
    }

    private void startShuffleManagerInternal() throws Exception {
        EnvironmentInformation.logEnvironmentInfo(LOG, "Shuffle Manager on Yarn", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        ShuffleManagerEntrypoint shuffleManagerEntrypoint = new ShuffleManagerEntrypoint(conf);
        ShuffleManagerEntrypoint.runShuffleManagerEntrypoint(shuffleManagerEntrypoint);
        LOG.info("Shuffle Manager on yarn starts successfully");

        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        AMRMClientAsync amRMClient =
                AMRMClientAsync.createAMRMClientAsync(
                        conf.getInteger(
                                YarnConstants.MANAGER_RM_HEARTBEAT_INTERVAL_MS_KEY,
                                YarnConstants.MANAGER_RM_HEARTBEAT_INTERVAL_MS_DEFAULT),
                        allocListener);
        amRMClient.init(new YarnConfiguration());
        amRMClient.start();

        NMCallbackHandler containerListener = new NMCallbackHandler();
        NMClientAsync nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(new YarnConfiguration());
        nmClientAsync.start();

        amRMClient.registerApplicationMaster(NetUtils.getHostname(), AM_RPC_PORT_DEFAULT, null);
    }

    public static void main(String[] args) {
        try {
            YarnShuffleManagerRunner yarnShuffleManagerRunner = new YarnShuffleManagerRunner(args);
            yarnShuffleManagerRunner.run();
        } catch (Throwable t) {
            LOG.error("Encountering a error when starting Shuffle Manager, ", t);
        }
    }
}
