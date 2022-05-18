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

package com.alibaba.flink.shuffle.storage.utils;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.exception.ConfigurationException;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.core.storage.StorageType;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkNotNull;

/** Utilities to parse the configuration of Storage layer. */
public class StorageConfigParseUtils {

    /** The result of parsing the configured paths. */
    public static class ParsedPathLists {

        private final List<String> ssdPaths;

        private final List<String> hddPaths;

        private final List<String> allPaths;

        public ParsedPathLists(
                List<String> ssdPaths, List<String> hddPaths, List<String> allPaths) {
            this.ssdPaths = checkNotNull(ssdPaths);
            this.hddPaths = checkNotNull(hddPaths);
            this.allPaths = checkNotNull(allPaths);
        }

        public List<String> getSsdPaths() {
            return ssdPaths;
        }

        public List<String> getHddPaths() {
            return hddPaths;
        }

        public List<String> getAllPaths() {
            return allPaths;
        }
    }

    /**
     * Parses the base paths configured by {@link StorageOptions#STORAGE_LOCAL_DATA_DIRS}
     *
     * <p>TODO: Will be replaced with a formal configuration object in the future.
     */
    public static ParsedPathLists parseStoragePaths(String directories) {
        List<String> ssdPaths = new ArrayList<>();
        List<String> hddPaths = new ArrayList<>();
        List<String> allPaths = new ArrayList<>();

        String[] paths = directories.split(",");
        for (String pathString : paths) {
            pathString = pathString.trim();
            if (pathString.equals("")) {
                continue;
            }

            if (pathString.startsWith("[SSD]")) {
                pathString = pathString.substring(5);
                pathString = pathString.endsWith("/") ? pathString : pathString + "/";
                ssdPaths.add(pathString);
            } else if (pathString.startsWith("[HDD]")) {
                pathString = pathString.substring(5);
                pathString = pathString.endsWith("/") ? pathString : pathString + "/";
                hddPaths.add(pathString);
            } else {
                // if no storage type is configured, HDD will be the default
                pathString = pathString.endsWith("/") ? pathString : pathString + "/";
                hddPaths.add(pathString);
            }
            allPaths.add(pathString);

            Path path = new File(pathString).toPath();
            if (!Files.exists(path)) {
                throw new ConfigurationException(
                        String.format(
                                "The data dir '%s' configured by '%s' does not exist.",
                                pathString, StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
            }

            if (!Files.isDirectory(path)) {
                throw new ConfigurationException(
                        String.format(
                                "The data dir '%s' configured by '%s' is not a directory.",
                                pathString, StorageOptions.STORAGE_LOCAL_DATA_DIRS.key()));
            }
        }

        return new ParsedPathLists(ssdPaths, hddPaths, allPaths);
    }

    /**
     * Helper method which parses the configuration and obtains the configured preferred storage
     * type.
     */
    public static StorageType confPreferredStorageType(Configuration configuration) {
        StorageType preferredStorageType;
        String diskTypeString = configuration.getString(StorageOptions.STORAGE_PREFERRED_TYPE);
        try {
            preferredStorageType = StorageType.valueOf(checkNotNull(diskTypeString).trim());
        } catch (Exception exception) {
            throw new ConfigurationException(
                    String.format(
                            "Illegal configured value %s for %s. Must be SSD, HDD or UNKNOWN.",
                            diskTypeString, StorageOptions.STORAGE_PREFERRED_TYPE.key()));
        }
        return preferredStorageType;
    }
}
