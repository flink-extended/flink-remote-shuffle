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

package com.alibaba.flink.shuffle.yarn.entry.worker;

import com.alibaba.flink.shuffle.common.config.Configuration;
import com.alibaba.flink.shuffle.common.utils.CommonUtils;
import com.alibaba.flink.shuffle.common.utils.FatalErrorExitUtils;
import com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner;
import com.alibaba.flink.shuffle.core.config.HighAvailabilityOptions;
import com.alibaba.flink.shuffle.core.config.MemoryOptions;
import com.alibaba.flink.shuffle.core.config.StorageOptions;
import com.alibaba.flink.shuffle.yarn.utils.DeployOnYarnUtils;
import com.alibaba.flink.shuffle.yarn.utils.YarnConstants;
import com.alibaba.flink.shuffle.yarn.utils.YarnOptions;

import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Yarn deployment entry point of shuffle worker runner. It should be deployed along with Yarn
 * NodeManager as a auxiliary service in NodeManager.
 *
 * <p>Before start it as an auxiliary service on Yarn, some specific configurations should be added
 * to yarn-site.xml: 1. Add yarn_remote_shuffle_worker_for_flink to yarn.nodemanager.aux-services.
 * 2. Set yarn.nodemanager.aux-services.yarn_remote_shuffle_worker_for_flink.class as {@link
 * YarnShuffleWorkerEntrypoint}. 3. Add HA mode options in {@link HighAvailabilityOptions}. 4. Add
 * memory in {@link MemoryOptions} and storage options in {@link StorageOptions}.
 *
 * <p>When starting Node Manager, the Hadoop classpath should contain all compiled remote shuffle
 * jars. Otherwise, {@link ClassNotFoundException} or other exceptions may be thrown out.
 */
public class YarnShuffleWorkerEntrypoint extends AuxiliaryService {
    private static final Logger LOG = LoggerFactory.getLogger(YarnShuffleWorkerEntrypoint.class);

    private static volatile ShuffleWorkerRunner shuffleWorkerRunner;

    public YarnShuffleWorkerEntrypoint() {
        super(YarnConstants.WORKER_AUXILIARY_SERVICE_NAME);
    }

    /** Starts the shuffle worker with the given configuration. */
    @Override
    protected void serviceInit(org.apache.hadoop.conf.Configuration hadoopConf) throws Exception {
        LOG.info("Initializing Shuffle Worker on Yarn for Flink");
        final boolean stopOnFailure;
        final Configuration configuration;
        try {
            configuration = Configuration.fromMap(DeployOnYarnUtils.hadoopConfToMaps(hadoopConf));
            stopOnFailure = configuration.getBoolean(YarnOptions.WORKER_STOP_ON_FAILURE);
        } catch (Throwable t) {
            LOG.error(
                    "Get configuration for "
                            + YarnOptions.WORKER_STOP_ON_FAILURE.key()
                            + " failed, ",
                    t);
            return;
        }

        FatalErrorExitUtils.setNeedStopProcess(stopOnFailure);

        try {
            shuffleWorkerRunner = ShuffleWorkerRunner.runShuffleWorker(configuration);
        } catch (Exception e) {
            LOG.error("Failed to start Shuffle Worker on Yarn for Flink, ", e);
            if (stopOnFailure) {
                throw e;
            } else {
                noteFailure(e);
            }
        } catch (Throwable t) {
            LOG.error(
                    "Failed to start Shuffle Worker on Yarn for Flink with the throwable error, ",
                    t);
        }
    }

    /** Currently this method is of no use. */
    @Override
    public void initializeApplication(ApplicationInitializationContext initAppContext) {}

    /** Currently this method is of no use. */
    @Override
    public void stopApplication(ApplicationTerminationContext stopAppContext) {}

    /** Close the shuffle worker. */
    @Override
    protected void serviceStop() {
        if (shuffleWorkerRunner == null) {
            return;
        }

        try {
            shuffleWorkerRunner.close();
        } catch (Exception e) {
            LOG.error("Close shuffle worker failed with error, ", e);
            return;
        }
        LOG.info("Stop shuffle worker normally.");
    }

    /** Currently this method is of no use. */
    @Override
    public ByteBuffer getMetaData() {
        return CommonUtils.allocateHeapByteBuffer(0);
    }

    @Override
    public String getName() {
        return YarnConstants.WORKER_AUXILIARY_SERVICE_NAME;
    }

    /** Only for tests. */
    public static ShuffleWorkerRunner getShuffleWorkerRunner() {
        return shuffleWorkerRunner;
    }
}
