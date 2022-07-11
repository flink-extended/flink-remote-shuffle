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

package com.alibaba.flink.shuffle.plugin.utils;

import com.alibaba.flink.shuffle.common.exception.ShuffleException;
import com.alibaba.flink.shuffle.coordinator.manager.DefaultShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleResource;
import com.alibaba.flink.shuffle.coordinator.manager.ShuffleWorkerDescriptor;
import com.alibaba.flink.shuffle.core.ids.DataSetID;
import com.alibaba.flink.shuffle.core.ids.JobID;
import com.alibaba.flink.shuffle.core.ids.MapPartitionID;
import com.alibaba.flink.shuffle.core.ids.ReducePartitionID;
import com.alibaba.flink.shuffle.plugin.RemoteShuffleDescriptor;
import com.alibaba.flink.shuffle.plugin.transfer.RemoteShuffleOutputGate;
import com.alibaba.flink.shuffle.transfer.ConnectionManager;
import com.alibaba.flink.shuffle.transfer.ShuffleWriteClient;

import org.apache.flink.runtime.shuffle.ShuffleDescriptor;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;

import static com.alibaba.flink.shuffle.common.utils.CommonUtils.checkState;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.HDD_MAP_PARTITION_FACTORY;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.HDD_REDUCE_PARTITION_FACTORY;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.MAP_PARTITION_FACTORY;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.REDUCE_PARTITION_FACTORY;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.SSD_MAP_PARTITION_FACTORY;
import static com.alibaba.flink.shuffle.core.utils.PartitionUtils.SSD_REDUCE_PARTITION_FACTORY;

/** Utils for intput gate or output gate. */
public class GateUtils {

    public static final String EMPTY_WRITE_CLIENT = "emptyClient";

    /**
     * Marker object used to signal the queue that the result partition is released or failed. This
     * is added to notify the queue that the result partition has been released or failed so that
     * the output gate will not be hanged when taking {@link ShuffleWriteClient} from the queue
     * writingClients of {@link RemoteShuffleOutputGate}.
     *
     * <p>Invariant: This is never on queue unless the result partition is released or failed.
     */
    public static final ShuffleWriteClient POISON =
            new ShuffleWriteClient(
                    InetSocketAddress.createUnresolved("localhost", 9999),
                    new JobID(EMPTY_WRITE_CLIENT.getBytes()),
                    new DataSetID(EMPTY_WRITE_CLIENT.getBytes()),
                    new MapPartitionID(EMPTY_WRITE_CLIENT.getBytes()),
                    new ReducePartitionID(0),
                    1,
                    1,
                    1,
                    false,
                    EMPTY_WRITE_CLIENT,
                    new ConnectionManager(null, null, 0, Duration.ofMillis(1)),
                    new ArrayList<ShuffleWriteClient>()::add);

    public static String getDataPartitionFactoryName(RemoteShuffleDescriptor rsd) {
        boolean isMapPartition = rsd.isMapPartition();
        ShuffleResource shuffleResource = rsd.getShuffleResource();
        String factoryName;
        if (isMapPartition) {
            switch (shuffleResource.getDiskType()) {
                case HDD_ONLY:
                    factoryName = HDD_MAP_PARTITION_FACTORY;
                    break;
                case SSD_ONLY:
                    factoryName = SSD_MAP_PARTITION_FACTORY;
                    break;
                case ANY_TYPE:
                    factoryName = MAP_PARTITION_FACTORY;
                    break;
                default:
                    throw new ShuffleException("No such disk type.");
            }
        } else {
            switch (shuffleResource.getDiskType()) {
                case HDD_ONLY:
                    factoryName = HDD_REDUCE_PARTITION_FACTORY;
                    break;
                case SSD_ONLY:
                    factoryName = SSD_REDUCE_PARTITION_FACTORY;
                    break;
                case ANY_TYPE:
                    factoryName = REDUCE_PARTITION_FACTORY;
                    break;
                default:
                    throw new ShuffleException("No such disk type.");
            }
        }
        return factoryName;
    }

    /**
     * Returns the simplified {@link ShuffleDescriptor}s according to the input shuffle descriptor
     * and the sub partition range.
     */
    public static ShuffleDescriptor[] getSimplifiedShuffleDescriptors(
            RemoteShuffleDescriptor shuffleDescriptor, int startSubIdx, int endSubIdx) {
        int numSubs = endSubIdx - startSubIdx + 1;
        ShuffleResource shuffleResource = shuffleDescriptor.getShuffleResource();
        ShuffleWorkerDescriptor[] shuffleWorkerDescriptors =
                shuffleResource.getReducePartitionLocations();
        checkState(shuffleWorkerDescriptors.length >= numSubs);
        ShuffleDescriptor[] simplifiedDescriptors = new ShuffleDescriptor[numSubs];

        for (int i = 0; i < numSubs; i++) {
            ShuffleWorkerDescriptor workerDescriptor = shuffleWorkerDescriptors[startSubIdx + i];
            DefaultShuffleResource simplifiedShuffleResource =
                    new DefaultShuffleResource(
                            new ShuffleWorkerDescriptor[] {workerDescriptor},
                            shuffleResource.getDataPartitionType(),
                            shuffleResource.getDiskType());
            simplifiedShuffleResource.setConsumerGroupID(shuffleResource.getConsumerGroupID());
            simplifiedDescriptors[i] =
                    new RemoteShuffleDescriptor(
                            shuffleDescriptor.getResultPartitionID(),
                            shuffleDescriptor.getJobId(),
                            simplifiedShuffleResource,
                            shuffleDescriptor.isMapPartition(),
                            shuffleDescriptor.getNumMaps());
        }
        return simplifiedDescriptors;
    }
}
