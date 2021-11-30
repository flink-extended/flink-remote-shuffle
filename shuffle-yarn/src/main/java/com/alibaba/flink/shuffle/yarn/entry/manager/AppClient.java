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

import com.alibaba.flink.shuffle.coordinator.manager.ShuffleManager;
import com.alibaba.flink.shuffle.coordinator.utils.EnvironmentInformation;
import com.alibaba.flink.shuffle.yarn.utils.DeployOnYarnUtils;
import com.alibaba.flink.shuffle.yarn.utils.YarnConstants;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

/**
 * Client for application submission to YARN.
 *
 * <p>This client is meant to submit an application to start a {@link YarnShuffleManagerRunner} as
 * an Application Master.
 */
public class AppClient {
    private static final Logger LOG = LoggerFactory.getLogger(AppClient.class);

    private final Configuration hadoopConf;

    private final YarnClient yarnClient;

    /** Main class to start application master. */
    private final String appMasterMainClass;

    private final AppClientEnvs appEnvs;

    private ApplicationId appId;

    /**
     * Use {@link YarnShuffleManagerRunner} as the application master main class. {@link
     * ShuffleManager} will be started in the container.
     */
    AppClient(String[] args) throws IOException, ParseException {
        this(args, new YarnConfiguration());
    }

    /** Privilege is set as public for testing. */
    public AppClient(String[] args, YarnConfiguration hadoopConf)
            throws IOException, ParseException {
        EnvironmentInformation.logEnvironmentInfo(LOG, "Yarn Shuffle Manager", args);
        this.appMasterMainClass = YarnShuffleManagerRunner.class.getCanonicalName();
        this.hadoopConf = hadoopConf;
        this.appEnvs = new AppClientEnvs(hadoopConf, args);
        this.hadoopConf.set(
                YarnConstants.MANAGER_AM_MAX_ATTEMPTS_KEY,
                String.valueOf(appEnvs.getAmMaxAttempts()));
        this.yarnClient = YarnClient.createYarnClient();
        yarnClient.init(hadoopConf);
    }

    /** Main method for submission. */
    boolean run() {
        boolean success;
        try {
            success = submitApplication();
        } catch (Exception e) {
            LOG.error("Submit application failed, ", e);
            success = false;
        }
        return success;
    }

    /** Privilege is set as public for testing. */
    public boolean submitApplication() throws IOException, YarnException, URISyntaxException {
        yarnClient.start();

        // Get a new application id
        final YarnClientApplication app = yarnClient.createApplication();

        // Set the application name
        final ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appId = appContext.getApplicationId();

        // Only one application master container is running in the application and there are no
        // other containers in it, so the keep-container flag is set as false
        appContext.setKeepContainersAcrossApplicationAttempts(false);
        appContext.setApplicationName(appEnvs.getAppName());

        // Set local resources for the application master
        final Map<String, LocalResource> localResources = new HashMap<>();

        final FileSystem fs = FileSystem.get(hadoopConf);

        prepareAppLocalResources(appId, localResources, fs);

        // Set up the container launch context for the application master
        final ContainerLaunchContext amContainer =
                ContainerLaunchContext.newInstance(
                        localResources, generateEnvs(), generateCommands(), null, null, null);

        // Setup resource capability according to the memory size and vcore count
        final Resource capability =
                Resource.newInstance(
                        appEnvs.getAmMemory() + appEnvs.getMemoryOverhead(), appEnvs.getAmVCores());
        appContext.setResource(capability);

        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
            addTokensToAmContainer(fs, amContainer);
        }

        appContext.setAMContainerSpec(amContainer);

        final Priority pri = Priority.newInstance(appEnvs.getAmPriority());
        appContext.setPriority(pri);

        // Set the application queue
        appContext.setQueue(appEnvs.getAmQueue());

        LOG.info("Submitting application, the app id is " + appId.toString());
        yarnClient.submitApplication(appContext);
        return true;
    }

    private void addTokensToAmContainer(FileSystem fs, ContainerLaunchContext amContainer)
            throws IOException {
        // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
        final Credentials credentials = new Credentials();
        final String tokenRenewer = hadoopConf.get(YarnConfiguration.RM_PRINCIPAL);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
            throw new IOException(
                    "Can't get Master Kerberos principal for the RM to use as renewer");
        }

        // For now, only getting tokens for the default file-system.
        final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
            for (Token<?> token : tokens) {
                LOG.info("Got delegation token for " + fs.getUri() + ", token: " + token);
            }
        }
        final DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        final ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        amContainer.setTokens(fsTokens);
    }

    private void prepareAppLocalResources(
            ApplicationId appId, Map<String, LocalResource> localResources, FileSystem fs)
            throws IOException, URISyntaxException {
        appEnvs.prepareDirAndFilesAmNeeded(appId);
        // Copy jars to the filesystem
        // Create a local resource to point to the destination jar path
        DeployOnYarnUtils.addFrameworkToDistributedCache(
                appEnvs.getShuffleHomeDirInHdfs(),
                localResources,
                LocalResourceType.ARCHIVE,
                YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME,
                hadoopConf);

        // Set the log4j properties if needed
        DeployOnYarnUtils.addFrameworkToDistributedCache(
                appEnvs.getLog4jPropertyFile(),
                localResources,
                LocalResourceType.FILE,
                YarnConstants.MANAGER_AM_LOG4J_FILE_NAME,
                hadoopConf);
    }

    /** Set the env variables to be setup in the env where the application master will be run. */
    private Map<String, String> generateEnvs() {
        final Map<String, String> env = new HashMap<>();
        final String classPaths = DeployOnYarnUtils.buildClassPathEnv(hadoopConf);
        env.put(YarnConstants.MANAGER_APP_ENV_CLASS_PATH_KEY, classPaths);
        LOG.info("Set the classpath for the application master, classpath: " + classPaths);
        return env;
    }

    private List<String> generateCommands() {
        // Set the necessary commands to execute the application master
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        vargs.add("echo classpath: $CLASSPATH;echo path: $PATH;");
        // Set java executable commands
        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx and Xms
        vargs.add("-Xmx" + appEnvs.getAmMemory() + "m");
        vargs.add("-Xms" + appEnvs.getAmMemory() + "m");
        // Set log4j properties
        vargs.add("-Dlog4j.configurationFile=" + YarnConstants.MANAGER_AM_LOG4J_FILE_NAME);
        // Set other jvm options if needed
        if (appEnvs.getAmJvmOptions() != null && !appEnvs.getAmJvmOptions().isEmpty()) {
            vargs.add(appEnvs.getAmJvmOptions());
        }
        vargs.add(
                " -cp $CLASSPATH:'./" + YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME + "/*'");
        // Set class name
        vargs.add(appMasterMainClass);
        vargs.add(
                "-D"
                        + YarnConstants.MANAGER_AM_APPID_TIMESTAMP_KEY
                        + "="
                        + appId.getClusterTimestamp());
        vargs.add("-D" + YarnConstants.MANAGER_AM_APPID_ID_KEY + "=" + appId.getId());
        // Set shuffle manager other options
        vargs.add(appEnvs.getShuffleManagerArgs());

        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        // Get final commmands
        final StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        LOG.info("Start Shuffle Manager by command: " + command.toString());
        final List<String> commands = new ArrayList<>();
        commands.add(command.toString());
        return commands;
    }
}
