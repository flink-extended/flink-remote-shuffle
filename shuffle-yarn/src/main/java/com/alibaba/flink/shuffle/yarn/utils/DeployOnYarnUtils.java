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

package com.alibaba.flink.shuffle.yarn.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/** Utility class that provides helper methods to work with Apache Hadoop YARN. */
public class DeployOnYarnUtils {
    private static final Logger LOG = LoggerFactory.getLogger(DeployOnYarnUtils.class);

    // ---------------------------------------------------------------
    // Configuration utils
    // ---------------------------------------------------------------

    private static final Option PARSE_PROPERTY_OPTION =
            Option.builder("D")
                    .argName("property=value")
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .desc("use value for given property")
                    .build();

    /** Parse hadoop configurations into maps. */
    public static Map<String, String> hadoopConfToMaps(
            final org.apache.hadoop.conf.Configuration conf) {
        Map<String, String> confMaps = new HashMap<>();
        for (Map.Entry<String, String> entry : conf) {
            confMaps.put(entry.getKey(), entry.getValue());
        }
        return confMaps;
    }

    /**
     * Parsing the input arguments into {@link Configuration}. The method may throw a {@link
     * ParseException} when inputting some wrong arguments.
     *
     * @param args The String array that shall be parsed.
     * @return The {@link Configuration} configurations.
     */
    public static Configuration parseParameters(String[] args) throws ParseException {
        final DefaultParser defaultParser = new DefaultParser();
        final Options options = new Options();
        options.addOption(PARSE_PROPERTY_OPTION);
        return new Configuration(
                defaultParser
                        .parse(options, args, true)
                        .getOptionProperties(PARSE_PROPERTY_OPTION.getOpt()));
    }

    // ---------------------------------------------------------------
    // Yarn utils
    // ---------------------------------------------------------------

    /** Build classpath string according to the input configurations. */
    public static String buildClassPathEnv(org.apache.hadoop.conf.Configuration conf) {
        StringBuilder classPathEnv =
                new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$PWD/" + YarnConstants.MANAGER_AM_LOG4J_FILE_NAME)
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$PWD")
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$HADOOP_CLIENT_CONF_DIR")
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$HADOOP_CONF_DIR")
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$JAVA_HOME/lib/tools.jar")
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$PWD/")
                        .append(YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME)
                        .append("/")
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$PWD/")
                        .append(YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME)
                        .append("/conf/")
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                        .append("$PWD/")
                        .append(YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME)
                        .append("/*");
        for (String c :
                conf.getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        return classPathEnv.toString();
    }

    public static void addFrameworkToDistributedCache(
            String javaPathInHdfs,
            Map<String, LocalResource> localResources,
            LocalResourceType resourceType,
            String resourceKey,
            org.apache.hadoop.conf.Configuration conf)
            throws IOException, URISyntaxException {
        FileSystem fs = FileSystem.get(conf);
        URI uri = getURIFromHdfsPath(javaPathInHdfs, resourceKey, conf);

        FileStatus scFileStatus = fs.getFileStatus(new Path(uri.getPath()));
        LocalResource scRsrc =
                LocalResource.newInstance(
                        ConverterUtils.getYarnUrlFromURI(uri),
                        resourceType,
                        LocalResourceVisibility.PRIVATE,
                        scFileStatus.getLen(),
                        scFileStatus.getModificationTime());
        localResources.put(resourceKey, scRsrc);
    }

    public static URI getURIFromHdfsPath(
            String inputPath, String resourceKey, org.apache.hadoop.conf.Configuration conf)
            throws IOException, URISyntaxException {
        URI uri = FileSystem.get(conf).resolvePath(new Path(inputPath)).toUri();
        return new URI(uri.getScheme(), uri.getAuthority(), uri.getPath(), null, resourceKey);
    }

    // ---------------------------------------------------------------
    // HDFS utils
    // ---------------------------------------------------------------

    /**
     * Uploading specific path into HDFS target directory. The target directory path will be
     * "fs.getHomeDirectory()/AM_REMOTE_SHUFFLE_DIST_DIR_NAME/appId/dstDir".
     */
    public static String uploadLocalDirToHDFS(
            FileSystem fs, String fileSrcPath, String appId, String fileDstPath)
            throws IOException {
        Path dst =
                new Path(
                        fs.getHomeDirectory(),
                        YarnConstants.MANAGER_AM_REMOTE_SHUFFLE_PATH_NAME
                                + "/"
                                + appId
                                + "/"
                                + fileDstPath);
        if (fs.exists(dst)) {
            throw new IOException("Upload files failed, because the path " + dst + " is exist.");
        }
        fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        LOG.info("Upload local " + fileSrcPath + " to " + dst);
        return dst.toString();
    }

    /**
     * Refactoring the target directory. The original directory has multiple sub directories, and
     * this method will move all files and jars in the sub directories into a new temporary
     * directory. The AM will be startup based on this new temporary directory.
     */
    public static String refactorDirectoryHierarchy(
            FileSystem fs, String shuffleHomeDir, org.apache.hadoop.conf.Configuration hadoopConf)
            throws IOException {
        Path targetDir = new Path(shuffleHomeDir);
        Path newTargetDir = new Path(shuffleHomeDir + "/" + YarnConstants.MANAGER_AM_TMP_PATH_NAME);
        if (!fs.exists(newTargetDir)) {
            fs.mkdirs(newTargetDir);
        }
        return refactorDirectoryHierarchyInternal(fs, targetDir, newTargetDir, hadoopConf)
                .toString();
    }

    private static Path refactorDirectoryHierarchyInternal(
            FileSystem fs,
            Path srcDir,
            Path targetDir,
            org.apache.hadoop.conf.Configuration hadoopConf)
            throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(srcDir);
        for (FileStatus fileStatus : fileStatuses) {
            if (fs.isDirectory(fileStatus.getPath())) {
                refactorDirectoryHierarchyInternal(fs, fileStatus.getPath(), targetDir, hadoopConf);
            } else {
                Path curPath = fileStatus.getPath();
                Path targetPath = new Path(targetDir, curPath.getName());
                FileUtil.copy(fs, curPath, fs, targetPath, false, hadoopConf);
            }
        }
        return targetDir;
    }

    /**
     * Find out the AM jar to start Application Master in the input directory. If not found, this
     * method will throw a {@link IOException}.
     */
    public static String findApplicationMasterJar(FileSystem fs, String targetDir)
            throws IOException {
        for (FileStatus fileStatus : listFileStatus(fs, targetDir)) {
            Path curPath = fileStatus.getPath();
            if (curPath.getName().startsWith(YarnConstants.MANAGER_AM_JAR_FILE_PREFIX)
                    && !curPath.getName()
                            .endsWith(YarnConstants.MANAGER_AM_JAR_FILE_EXCLUDE_SUFFIX)) {
                return curPath.toString();
            }
        }
        throw new IOException("Can not find application master jar in the directory " + targetDir);
    }

    /**
     * Find out the log4j properties file in the input directory. If not found, this method will
     * throw a {@link IOException}.
     */
    public static String findLog4jPropertyFile(FileSystem fs, String targetDir) throws IOException {
        for (FileStatus fileStatus : listFileStatus(fs, targetDir)) {
            Path curPath = fileStatus.getPath();
            if (curPath.getName().equals(YarnConstants.MANAGER_AM_LOG4J_FILE_NAME)) {
                return curPath.toString();
            }
        }

        throw new IOException(
                "Can not find "
                        + YarnConstants.MANAGER_AM_LOG4J_FILE_NAME
                        + " in the directory "
                        + targetDir);
    }

    private static FileStatus[] listFileStatus(FileSystem fs, String targetDir) throws IOException {
        return fs.listStatus(new Path(targetDir));
    }
}
