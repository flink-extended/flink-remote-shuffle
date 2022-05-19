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
import com.alibaba.flink.shuffle.yarn.utils.DeployOnYarnUtils;
import com.alibaba.flink.shuffle.yarn.utils.YarnConstants;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;
import static com.alibaba.flink.shuffle.core.utils.ConfigurationParserUtils.DEFAULT_SHUFFLE_CONF_DIR;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_MEMORY_JVM_OPTIONS_DEFAULT;
import static com.alibaba.flink.shuffle.yarn.utils.YarnConstants.MANAGER_AM_MEMORY_JVM_OPTIONS_KEY;

/**
 * Arg parser used by {@link AppClient}. The class will parse all arguments in {@link YarnConstants}
 */
public class AppClientEnvs {
    private static final Logger LOG = LoggerFactory.getLogger(AppClientEnvs.class);

    private final org.apache.hadoop.conf.Configuration hadoopConf;

    private final FileSystem fs;

    /** Local home dir contains all jars, confs etc. */
    private final String localShuffleHomeDir;

    /** Application name to start Shuffle Manager. */
    private final String appName;

    /** Application master priority. */
    private final int amPriority;

    /** Queue for Application master. */
    private final String amQueue;

    /** Application master max attempts count when encountering a exception. */
    private final int amMaxAttempts;

    /** Virtual core count to start Application master. */
    private final int amVCores;

    /** Other JVM options of application master. */
    private final String amJvmOptions;

    /** Memory resource to start Application master. */
    private int amMemory;

    /** Overhead memory to start Application master. */
    private int memoryOverhead;

    /**
     * Shuffle manager args. Specified by pattern of -D remote-shuffle.yarn.manager-start-opts="-D
     * a.b.c1=v1 -D a.b.c2=v2".
     */
    private final StringBuilder shuffleManagerArgs;

    /**
     * When the input arguments are not illegal, the map will store the overridden keys and values.
     */
    private Map<String, Integer> overrideOptions;

    /**
     * Application directory in HDFS which contains all jars and configurations. The directory path
     * is "fs.getHomeDirectory()/appName/appId/MANAGER_HOME_DIR/AM_HDFS_TMP_DIR".
     */
    private String shuffleHomeDirInHdfs;

    /**
     * Log4j properties file. The file should be contained in the specified MANAGER_HOME_DIR
     * directory.
     */
    private String log4jPropertyFile;

    /**
     * A HDFS jar for starting App Master. The jar files should be contained in the specified
     * MANAGER_HOME_DIR directory.
     */
    private String amJarFilePath;

    public AppClientEnvs(final org.apache.hadoop.conf.Configuration hadoopConf, final String[] args)
            throws IOException, ParseException {
        Configuration conf = DeployOnYarnUtils.parseParameters(args);
        this.localShuffleHomeDir = checkNotNull(conf.getString(YarnConstants.MANAGER_HOME_DIR));
        loadOptionsInConfigurationFile(conf, localShuffleHomeDir);

        this.hadoopConf = hadoopConf;
        this.fs = FileSystem.get(hadoopConf);
        this.amVCores = YarnConstants.MANAGER_AM_VCORE_COUNT;
        this.appName =
                conf.getString(
                        YarnConstants.MANAGER_APP_NAME_KEY, YarnConstants.MANAGER_APP_NAME_DEFAULT);
        this.amPriority =
                conf.getInteger(
                        YarnConstants.MANAGER_APP_PRIORITY_KEY,
                        YarnConstants.MANAGER_APP_PRIORITY_DEFAULT);
        this.amQueue =
                conf.getString(
                        YarnConstants.MANAGER_APP_QUEUE_NAME_KEY,
                        YarnConstants.MANAGER_APP_QUEUE_NAME_DEFAULT);
        this.amMemory =
                conf.getInteger(
                        YarnConstants.MANAGER_AM_MEMORY_SIZE_KEY,
                        YarnConstants.MANAGER_AM_MEMORY_SIZE_DEFAULT);
        this.memoryOverhead =
                conf.getInteger(
                        YarnConstants.MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY,
                        YarnConstants.MANAGER_AM_MEMORY_OVERHEAD_SIZE_DEFAULT);
        this.amMaxAttempts =
                conf.getInteger(
                        YarnConstants.MANAGER_AM_MAX_ATTEMPTS_VAL_KEY,
                        YarnConstants.MANAGER_AM_MAX_ATTEMPTS_VAL_DEFAULT);
        this.amJvmOptions =
                conf.getString(
                        MANAGER_AM_MEMORY_JVM_OPTIONS_KEY, MANAGER_AM_MEMORY_JVM_OPTIONS_DEFAULT);
        this.shuffleManagerArgs = new StringBuilder();
        this.overrideOptions = new HashMap<>();

        // Check the input arguments, if any argument is wrong, modify it or throw an exception.
        checkArguments();
        generateShuffleManagerArgString(conf);
    }

    private static void loadOptionsInConfigurationFile(Configuration conf, String localHomeDir)
            throws IOException {
        String confFile = localHomeDir + "/" + DEFAULT_SHUFFLE_CONF_DIR;
        Configuration confFromFile = new Configuration(confFile);
        conf.addAll(confFromFile);
        LOG.info("Loaded " + confFromFile.toMap().size() + " options from " + confFile);
    }

    /**
     * Refactor the Yarn home directory containing jars and other resources. Move all libs and
     * configurations to a new temporary directory. After reorganizing the input directory, we
     * should find out the main executed jar, log4j properties file, etc.
     */
    public void prepareDirAndFilesAmNeeded(ApplicationId appId) throws IOException {
        String remoteShuffleDir =
                DeployOnYarnUtils.uploadLocalDirToHDFS(
                        fs,
                        localShuffleHomeDir,
                        appId.toString(),
                        YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME);
        shuffleHomeDirInHdfs =
                DeployOnYarnUtils.refactorDirectoryHierarchy(fs, remoteShuffleDir, hadoopConf);
        amJarFilePath = DeployOnYarnUtils.findApplicationMasterJar(fs, shuffleHomeDirInHdfs);
        log4jPropertyFile = DeployOnYarnUtils.findLog4jPropertyFile(fs, shuffleHomeDirInHdfs);
        LOG.info(
                "Remote shuffle home directory in HDFS is "
                        + shuffleHomeDirInHdfs
                        + ", AM jar is "
                        + amJarFilePath
                        + ", log4j properties file is "
                        + log4jPropertyFile);
    }

    private void checkArguments() {
        if (amVCores < 1) {
            throw new IllegalArgumentException(
                    "Invalid virtual cores specified for application master, exiting.");
        }

        if (localShuffleHomeDir == null) {
            throw new IllegalArgumentException(
                    "Shuffle local home directory is not specified. The specified directory path "
                            + "should contains all jars and files, e.g. lib, conf, log. Specify it by "
                            + YarnConstants.MANAGER_HOME_DIR);
        }

        if (amMemory < YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB) {
            LOG.warn(
                    "The input AM memory size is too small, the minimum size "
                            + YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB
                            + " mb will be used.");
            amMemory = YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB;
            overrideOptions.put(YarnConstants.MANAGER_AM_MEMORY_SIZE_KEY, amMemory);
        }

        if (memoryOverhead < YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB) {
            LOG.warn(
                    "The input overhead memory size is too small, the minimum size "
                            + YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB
                            + " mb will be used.");
            memoryOverhead = YarnConstants.MIN_VALID_AM_MEMORY_SIZE_MB;
            overrideOptions.put(YarnConstants.MANAGER_AM_MEMORY_OVERHEAD_SIZE_KEY, memoryOverhead);
        }
    }

    private void generateShuffleManagerArgString(final Configuration conf) {
        Map<String, String> confMap = conf.toMap();
        for (String optionKey : confMap.keySet()) {
            shuffleManagerArgs.append(" -D ").append(optionKey).append("=");
            if (overrideOptions.containsKey(optionKey)) {
                shuffleManagerArgs.append(overrideOptions.get(optionKey));
            } else {
                shuffleManagerArgs.append(confMap.get(optionKey));
            }
        }
    }

    String getAppName() {
        return appName;
    }

    int getAmPriority() {
        return amPriority;
    }

    String getAmQueue() {
        return amQueue;
    }

    int getAmMemory() {
        return amMemory;
    }

    int getMemoryOverhead() {
        return memoryOverhead;
    }

    int getAmMaxAttempts() {
        return amMaxAttempts;
    }

    int getAmVCores() {
        return amVCores;
    }

    String getAmJvmOptions() {
        return amJvmOptions;
    }

    String getShuffleHomeDirInHdfs() {
        return shuffleHomeDirInHdfs;
    }

    String getShuffleManagerArgs() {
        return shuffleManagerArgs.toString();
    }

    String getLog4jPropertyFile() {
        return log4jPropertyFile;
    }
}
