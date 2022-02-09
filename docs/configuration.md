<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Configuration

- [Options for Flink Cluster](#options-for-flink-cluster)
    - [Data Transmission Related (Client)](#data-transmission-related-(client))
    - [ShuffleMaster Related](#shufflemaster-related)
    - [High Availability Related (Client)](#high-availability-related-(client))
- [Options for Shuffle Cluster](#options-for-shuffle-cluster)
    - [High Availability Related (Server)](#high-availability-related-(server))
    - [RPC & Heartbeat Related](#rpc--heartbeat-related)
    - [ShuffleWorker Related](#shuffleworker-related)
    - [ShuffleManager Related](#shufflemanager-related)
    - [Data Transmission Related (Server)](#data-transmission-related-(server))
    - [Metric & Rest Related](#metric--rest-related)
- [Options for Deployment](#options-for-deployment)
    - [K8s Deployment Related](#k8s-deployment-related)
    - [Yarn Deployment Related](#yarn-deployment-related)
    - [Standalone Deployment Related](#standalone-deployment-related)

This section will present all valid config options that can be used by the remote shuffle cluster
together with the corresponding Flink cluster (jobs using the remote shuffle). Among these config
options, some are required which means you must give a config value, some are optional and the
default values are usually good enough for most cases.

For the configuration used by Flink cluster, you can just put them in the Flink configuration file,
please refer to the Flink document for more information.

For the configuration used by the remote shuffle cluster, there are different ways to config them
depending on the deployment type:

1. For standalone and local deployment, you should put the customized configuration in the remote
   shuffle configuration file conf/remote-shuffle-conf.yaml with the format key: value.

2. For yarn deployment, the `ShuffleManager` configuration should be put in the remote shuffle
   configuration file conf/remote-shuffle-conf.yaml and the `ShuffleWorker` configuration should be
   put in the yarn-site.xml.

3. For k8s deployment, you should put the customized configuration in the k8s deployment yaml file.

Please read the following part and refer to the deployment
guide ([Standalone](./deploy_standalone_mode.md)
, [Yarn](./deploy_on_yarn.md)
, [Kubernetes](./deploy_on_kubernetes.md))
for more information.

**Important:** To use the remote shuffle service in Flink, you must put the following configuration
in the Flink configuration file:

```yaml
shuffle-service-factory.class: com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory
```

## Options for Flink Cluster

This section will present the valid config options that can be used by the Flink cluster and should
be put in the Flink configuration file.

### Data Transmission Related (Client)

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.job.memory-per-partition` | MemorySize | `64m` | 1.0.0 | false | The size of network buffers required per result partition. The minimum valid value is 8M. Usually, several hundreds of megabytes memory is enough for large scale batch jobs. |
| `remote-shuffle.job.memory-per-gate` | MemorySize | `32m` | 1.0.0 | false | The size of network buffers required per input gate. The minimum valid value is 8m. Usually, several hundreds of megabytes memory is enough for large scale batch jobs. |
| `remote-shuffle.job.enable-data-compression` | Bool | `true` | 1.0.0 | false | Whether to enable shuffle data compression. Usually, enabling data compression can save the storage space and achieve better performance. |
| `remote-shuffle.job.data-partition-factory-name` | String | `com.alibaba.flink.shuffle.storage.partition.LocalFileMapPartitionFactory` | 1.0.0 | false | Defines the factory used to create new data partitions. According to the specified data partition factory from the client side, the `ShuffleManager` will return corresponding resources and the `ShuffleWorker` will create the corresponding partitions. All supported data partition factories can be found in the [data storage](./user_guide.md#data-storage) section. |
| `remote-shuffle.job.concurrent-readings-per-gate` | Integer | `2147483647` | 1.0.0 | false | The maximum number of remote shuffle channels to open and read concurrently per input gate. |
| `remote-shuffle.transfer.server.data-port` | Integer | `10086` | 1.0.0 | false | Data port to write shuffle data to and read shuffle data from `ShuffleWorker`s. This port must be accessible from the Flink cluster. |
| `remote-shuffle.transfer.client.num-threads` | Integer | `-1` | 1.0.0 | false | The number of Netty threads to be used at the client (flink job) side. The default `-1` means that `2 * (the number of slots)` will be used. |
| `remote-shuffle.transfer.client.connect-timeout` | Duration | `2min` | 1.0.0 | false | The TCP connection setup timeout of the Netty client. |
| `remote-shuffle.transfer.client.connect-retries` | Integer | `3` | 1.0.0 | false | Number of retries when failed to connect to the remote `ShuffleWorker`. |
| `remote-shuffle.transfer.client.connect-retry-wait` | Duration | `3s` | 1.0.0 | false | Time to wait between two consecutive connection retries. |
| `remote-shuffle.transfer.transport-type` | String | `auto` | 1.0.0 | false | The Netty transport type, either `nio` or `epoll`. The `auto` means "selecting the proper mode automatically based on the platform. Note that the `epoll` mode can get better performance, less GC and have more advanced features which are only available on modern Linux. |
| `remote-shuffle.transfer.send-receive-buffer-size` | MemorySize | `0b` | 1.0.0 | false | The Netty send and receive buffer size. The default `0` means the system buffer size (cat /proc/sys/net/ipv4/tcp_[rw]mem) and is 4 MiB in modern Linux. |
| `remote-shuffle.transfer.heartbeat.interval` | Duration | `1min` | 1.0.0 | false | The time interval to send heartbeat between the Netty server and Netty client. |
| `remote-shuffle.transfer.heartbeat.timeout` | Duration | `5min` | 1.0.0 | false | Heartbeat timeout used to detect broken Netty connections. |

### ShuffleMaster Related

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.worker.max-recovery-time` | Duration | `3min` | 1.0.0 | false | Maximum time to wait before reproducing the data stored in the lost worker (heartbeat timeout). The lost worker may become available again in this timeout. |
| `remote-shuffle.client.heartbeat.interval` | Duration | `10s` | 1.0.0 | false | Time interval for `ShuffleClient` （running in `ShuffleMaster`） to request heartbeat from `ShuffleManager`. |
| `remote-shuffle.client.heartbeat.timeout` | Duration | `120s` | 1.0.0 | false | Timeout for `ShuffleClient` (running in `ShuffleMaster`) and `ShuffleManager` to request and receive heartbeat. |
| `remote-shuffle.rpc.timeout` | Duration | `30s` | 1.0.0 | false | Timeout for `ShuffleClient` (running in `ShuffleMaster`) <-> `ShuffleManager` rpc calls. |
| `remote-shuffle.rpc.akka-frame-size` | String | `10485760b` | 1.0.0 | false | Maximum size of messages can be sent through rpc calls. |
| `remote-shuffle.cluster.registration.timeout` | Duration | `5min` | 1.0.0 | false | Defines the timeout for the `ShuffleClient` (running in `ShuffleMaster`) registration to the `ShuffleManager`. If the duration is exceeded without a successful registration, then the `ShuffleClient` terminates which will lead to the termination of the Flink AM. |
| `remote-shuffle.cluster.registration.error-delay` | Duration | `10s` | 1.0.0 | false | The pause made after a registration attempt caused an exception (other than timeout). |
| `remote-shuffle.cluster.registration.refused-delay` | Duration | `30s` | 1.0.0 | false | The pause made after the registration attempt was refused. |

### High Availability Related (Client)

| Key | Value Type | Default Value | Version | Required | Description | 
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.high-availability.mode` | String | `NONE` | 1.0.0 | false (Must be set if you want to enable HA) | Defines high-availability mode used for the cluster execution. To enable high-availability, set this mode to `ZOOKEEPER` or specify FQN of factory class. |
| `remote-shuffle.ha.zookeeper.quorum` | String | `null` | 1.0.0 | false (Must be set if high-availability mode is ZOOKEEPER) | The ZooKeeper quorum to use when running the remote shuffle cluster in a high-availability mode with ZooKeeper. |
| `remote-shuffle.ha.zookeeper.root-path` | String | `flink-remote-shuffle` | 1.0.0 | false | The root path in ZooKeeper under which the remote shuffle cluster stores its entries. Different remote shuffle clusters will be distinguished by the cluster id. This config must be consistent between the Flink cluster side and the shuffle cluster side. |
| remote-shuffle.ha.zookeeper.session-timeout | Duration | `60s` | 1.0.0 | false | Defines the session timeout for the ZooKeeper session. |
| `remote-shuffle.ha.zookeeper.connection-timeout` | Duration | `15s` | 1.0.0 | false | Defines the connection timeout for the ZooKeeper client. |
| `remote-shuffle.ha.zookeeper.retry-wait` | Duration | `5s` | 1.0.0 | false | Defines the pause between consecutive connection retries. |
| `remote-shuffle.ha.zookeeper.max-retry-attempts` | Integer | `3` | 1.0.0 | false | Defines the number of connection retries before the client gives up. |

## Options for Shuffle Cluster

This section will present the valid config options that can be used by the shuffle cluster. Note:
Where to put the customized configuration depends on the deployment type. For k8s deployment, you
should put these in the k8s deployment yaml file. For yarn deployment, you should put these in the
yarn-site.xml for the `ShuffleWorker`s and in the conf/remote-shuffle-conf.yaml file for
the `ShuffleManager`. For standalone and local deployment, you should put these in the
conf/remote-shuffle-conf.yaml file.

### High Availability Related (Server)

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.cluster.id` | String | `/default-cluster` | 1.0.0 | false | The unique ID of the remote shuffle cluster used by high-availability. Different shuffle clusters sharing the same zookeeper instance must be configured with different cluster id. This config must be consistent between the `ShuffleManager` and `ShuffleWorker`s. |
| `remote-shuffle.high-availability.mode` | String | `NONE` | 1.0.0 | false (Must be set if you want to enable HA) | Defines high-availability mode used for the cluster execution. To enable high-availability, set this mode to `ZOOKEEPER` or specify FQN of factory class. |
| `remote-shuffle.ha.zookeeper.quorum` | String | `null` | 1.0.0 | false (Must be set if high-availability mode is ZOOKEEPER) | The ZooKeeper quorum to use when running the remote shuffle cluster in a high-availability mode with ZooKeeper. |
| `remote-shuffle.ha.zookeeper.root-path` | String | `flink-remote-shuffle` | 1.0.0 | false | The root path in ZooKeeper under which the remote shuffle cluster stores its entries. Different remote shuffle clusters will be distinguished by the cluster id. This config must be consistent between the Flink cluster side and the shuffle cluster side. |
| `remote-shuffle.ha.zookeeper.session-timeout` | Duration | `60s` | 1.0.0 | false | Defines the session timeout for the ZooKeeper session. |
| `remote-shuffle.ha.zookeeper.connection-timeout` | Duration | `15s` | 1.0.0 | false | Defines the connection timeout for the ZooKeeper client. |
| `remote-shuffle.ha.zookeeper.retry-wait` | Duration | `5s` | 1.0.0 | false | Defines the pause between consecutive connection retries. |
| `remote-shuffle.ha.zookeeper.max-retry-attempts` | Integer | `3` | 1.0.0 | false | Defines the number of connection retries before the client gives up. |

### RPC & Heartbeat Related

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.worker.heartbeat.interval` | Duration | `10s` | 1.0.0 | false | Time interval for `ShuffleManager` to request heartbeat from `ShuffleWorker`s. |
| `remote-shuffle.worker.heartbeat.timeout` | Duration | `60s` | 1.0.0 | false | Timeout for `ShuffleManager` and `ShuffleWorker` to request and receive heartbeat. |
| `remote-shuffle.rpc.timeout` | Duration | `30s` | 1.0.0 | false | Timeout for `ShuffleWorker` <-> `ShuffleManager` rpc calls. |
| `remote-shuffle.rpc.akka-frame-size` | String | `10485760b` | 1.0.0 | false | Maximum size of messages can be sent through rpc calls. |

### ShuffleWorker Related

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.storage.local-data-dirs` | String | `null` | 1.0.0 | true | Local file system directories to persist partitioned data to. Multiple directories can be configured and these directories should be separated by comma (,). Each configured directory can be attached with an optional label which indicates the disk type. The valid disk types include `SSD` and `HDD`. If no label is offered, the default type would be `HDD`. Here is a simple valid configuration example: **`[SSD]/dir1/,[HDD]/dir2/,/dir3/`**. This option must be configured and the configured directories must exist. |
| `remote-shuffle.storage.min-reserved-space-bytes` | MemorySize | `5g` | 1.1.0 | false | Minimum reserved space size per disk for shuffle workers. This option is used to filter out the workers with small remaining storage space to ensure that the storage space will not be exhausted to avoid the 'No space left on device' exception. The default value 5g is pretty small to avoid wasting too large storage resources and for production usage, we suggest setting this to a larger value, for example, 50g. |
| `remote-shuffle.storage.check-update-period` | Duration | `10s` | 1.1.0 | false | The update interval of the worker check thread which will periodically get the status like disk space at this time interval. |
| `remote-shuffle.storage.enable-data-checksum` | Bool | `false` | 1.0.0 | false | Whether to enable data checksum for data integrity verification or not. |
| `remote-shuffle.memory.data-writing-size` | MemorySize | `4g` | 1.0.0 | 1.0.0 | Size of memory to be allocated for data writing. Larger value means more direct memory consumption which may lead to better performance. The configured value must be no smaller than `64m` and the buffer size configured by `remote-shuffle.memory.buffer-size`, otherwise an exception will be thrown. |
| `remote-shuffle.memory.data-reading-size` | MemorySize | `4g` | 1.0.0 | 1.0.0 | Size of memory to be allocated for data reading. Larger value means more direct memory consumption which may lead to better performance. The configured value must be no smaller than `64m` and the buffer size configured by `remote-shuffle.memory.buffer-size`, otherwise an exception will be thrown. |
| `remote-shuffle.memory.buffer-size` | MemorySize | `32k` | 1.0.0 | false | Size of the buffer to be allocated. Those allocated buffers will be used by both network and storage for data transmission, data writing and data reading. |
| `remote-shuffle.storage.preferred-disk-type` | String | `SSD` | 1.0.0 | false | Preferred disk type to use for data storage. The valid types include `SSD` and `HDD`. If there are disks of the preferred type, only those disks will be used. However, this is not a strict restriction, which means if there is no disk of the preferred type, disks of other types will be also used. |
| `remote-shuffle.storage.hdd.num-executor-threads` | Integer | `8` | 1.0.0 | false | Number of threads to be used by data store for data partition processing of each HDD. The actual number of threads per disk will be `min[configured value, 4 * (number of processors)]`. |
| `remote-shuffle.storage.ssd.num-executor-threads` | Integer | `2147483647` | 1.0.0 | false | Number of threads to be used by data store for data partition processing of each SSD. The actual number of threads per disk will be `min[configured value, 4 * (number of processors)]`. |
| `remote-shuffle.storage.partition.max-writing-memory` | MemorySize | `128m` | 1.0.0 | false | Maximum memory size to use for the data writing of each data partition. Note that if the configured value is smaller than 16m, the minimum 16m will be used. |
| `remote-shuffle.storage.partition.max-reading-memory` | MemorySize | `128m` | 1.0.0 | false | Maximum memory size to use for the data reading of each data partition. Note that if the configured value is smaller than 16m, the minimum 16m will be used. |
| `remote-shuffle.storage.file-tolerable-failures` | Integer | `2147483647` | 1.0.0 | false | Maximum number of tolerable failures before marking a data partition as corrupted, which will trigger the reproduction of the corresponding data. |
| `remote-shuffle.cluster.registration.timeout` | Duration | `5min` | 1.0.0 | false | Defines the timeout for the `ShuffleWorker` registration to the `ShuffleManager`. If the duration is exceeded without a successful registration, then the `ShuffleWorker` terminates. |
| `remote-shuffle.cluster.registration.error-delay` | Duration | `10s` | 1.0.0 | false | The pause made after a registration attempt caused an exception (other than timeout). |
| `remote-shuffle.cluster.registration.refused-delay` | Duration | `30s` | 1.0.0 | false | The pause made after the registration attempt was refused. |
| `remote-shuffle.worker.host` | String | `null` | 1.0.0 | false | The external address of the network interface where the `ShuffleWorker` is exposed. If not set, it will be determined automatically. Note: Different workers may need different values for this option, usually it can be specified in a non-shared `ShuffleWorker` specific configuration file. |
| `remote-shuffle.worker.bind-policy` | String | `ip` | 1.0.0 | false | The automatic address binding policy used by the `ShuffleWorker` if `remote-shuffle.worker.host` is not set. The valid types include `name` and `ip`: `name` means using hostname as binding address, `ip` means using host's ip address as binding address. |
| `remote-shuffle.worker.bind-host` | String | `0.0.0.0` | 1.0.0 | false | The local address of the network interface that the `ShuffleWorker` binds to. |
| `remote-shuffle.worker.rpc-port` | String | `0` | 1.0.0 | false | Defines network port range the `ShuffleWorker` expects incoming RPC connections. Accepts a list of ports (“50100,50101”), ranges (“50100-50200”) or a combination of both. The default `0` means that the `ShuffleWorker` will search for a free port itself. |
| `remote-shuffle.worker.rpc-bind-port` | Integer | `null` | 1.0.0 | false | The local network port that the `ShuffleWorker` binds to. If not configured, the external port (configured by `remote-shuffle.worker.rpc-port`) will be used. |

### ShuffleManager Related

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.manager.rpc-address` | String | `null` | 1.0.0 | false | Defines the external network address to connect to for communication with the `ShuffleManager`. |
| `remote-shuffle.manager.rpc-bind-address` | String | `null` | 1.0.0 | false | The local address of the network interface that the `ShuffleManager` binds to. |
| `remote-shuffle.manager.rpc-port` | Integer | `23123` | 1.0.0 | false | Defines the external network port to connect to for communication with the `ShuffleManager`. |
| `remote-shuffle.manager.rpc-bind-port` | Integer | `null` | 1.0.0 | false | The local network port that the `ShuffleManager` binds to. If not configured, the external port (configured by `remote-shuffle.manager.rpc-port` ) will be used. |
| `remote-shuffle.manager.rpc-port` | Integer | `23123` | 1.0.0 | false | Defines the external network port to connect to for communication with the `ShuffleManager`. |

### Data Transmission Related (Server)

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.transfer.transport-type` | String | `auto` | 1.0.0 | false | The Netty transport type, either `nio` or `epoll`. The `auto` means "selecting the proper mode automatically based on the platform. Note that the `epoll` mode can get better performance, less GC and have more advanced features which are only available on modern Linux. |
| `remote-shuffle.transfer.send-receive-buffer-size` | MemorySize | `0b` | 1.0.0 | false | The Netty send and receive buffer size. The default `0b` means the system buffer size (cat /proc/sys/net/ipv4/tcp_[rw]mem) and is 4 MiB in modern Linux. |
| `remote-shuffle.transfer.heartbeat.interval` | Duration | `1min` | 1.0.0 | false | The time interval to send heartbeat between the Netty server and Netty client. |
| `remote-shuffle.manager.partition-placement-strategy` | String | `round-robin` | 1.1.0 | false | Worker selection strategy deciding which worker the next data partition to store. Different selection strategies can be specified through this option: `min-num`: select the next worker with minimum number of data partitions; `random`: select the next worker in random order; `round-robin`: select the next worker in round-robin order; `locality`: select the local worker first, if not satisfied, select other remote workers in round-robin order. |

### Metric & Rest Related

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.rest.bind-host` | String | `0.0.0.0` | 1.0.0 | false | Local address of the network interface that the rest server binds to. |
| `remote-shuffle.rest.manager.bind-port` | Integer | `23101` | 1.0.0 | false | `ShuffleManager` rest server bind port. |
| `remote-shuffle.rest.worker.bind-port` | Integer | `23103` | 1.0.0 | false | `ShuffleWorker` rest server bind port. |

## Options for Deployment

### K8s Deployment Related

This section will present the valid config option that can be used by k8s deployment and should be
put in the k8s deployment yaml file.

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.kubernetes.container.image` | String | `null` | 1.0.0 | false (Must be set if running in k8s environment) | Image to use for the remote `ShuffleManager` and worker containers. |
| `remote-shuffle.kubernetes.container.image.pull-policy` | String | `IfNotPresent` | 1.0.0 | false | The Kubernetes container image pull policy (`IfNotPresent` or `Always` or `Never`). The default policy is IfNotPresent to avoid putting pressure to image repository. |
| `remote-shuffle.kubernetes.host-network.enabled` | Bool | `true` | 1.0.0 | false | Whether to enable host network for pod. Generally, host network is faster. |
| `remote-shuffle.kubernetes.manager.cpu` | Double | `1.0` (It is better to increase this value for production usage) | 1.0.0 | false | The number of cpu used by the `ShuffleManager`. |
| `remote-shuffle.kubernetes.manager.env-vars` | String | `''` | 1.0.0 | true | Env vars for the `ShuffleManager`. Specified as key:value pairs separated by commas. You need to specify the right timezone through this config option, for example, set timezone as TZ:Asia/Shanghai. |
| `remote-shuffle.kubernetes.manager.labels` | String | `''` | 1.0.0 | false | The user-specified labels to be set for the `ShuffleManager` pod. Specified as key:value pairs separated by commas. For example, `version:alphav1,deploy:test`. |
| `remote-shuffle.kubernetes.manager.node-selector` | String | `''` | 1.0.0 | false | The user-specified node selector to be set for the `ShuffleManager` pod. Specified as key:value pairs separated by commas. For example, `environment:production,disk:ssd`. |
| `remote-shuffle.kubernetes.manager.tolerations` | String | `''` | 1.0.0 | false | The user-specified tolerations to be set to the `ShuffleManager` pod. The value should be in the form of `key:key1,operator:Equal,value:value1,effect:NoSchedule;key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000`. |
| `remote-shuffle.kubernetes.worker.cpu` | Double | `1.0` (It is better to increase this value for production usage) | 1.0.0 | false | The number of cpu used by the `ShuffleWorker`. |
| `remote-shuffle.kubernetes.worker.env-vars` | String | `''` | 1.0.0 | true | Env vars for the `ShuffleWorker`. Specified as key:value pairs separated by commas. You need to specify the right timezone through this config option, for example, set timezone as `TZ:Asia/Shanghai`. |
| `remote-shuffle.kubernetes.worker.volume.empty-dirs` | String | `''` | 1.0.0 | false | Specify the kubernetes empty dir volumes that will be mounted into `ShuffleWorker` container. The value should be in form of `name:disk1,sizeLimit:5Gi,mountPath:/opt/disk1;name:disk2,sizeLimit:5Gi,mountPath:/opt/disk2. More specifically`, `name` is the name of the volume, `sizeLimit` is the limit size of the volume and `mountPath` is the mount path in container. |
| `remote-shuffle.kubernetes.worker.volume.host-paths` | String | `''` | 1.0.0 | false (Either this or `remote-shuffle.kubernetes.worker.volume.empty-dirs` must be configured for k8s deployment) | Specify the kubernetes HostPath volumes that will be mounted into `ShuffleWorker` container. The value should be in form of `name:disk1,path:/dump/1,mountPath:/opt/disk1;name:disk2,path:/dump/2,mountPath:/opt/disk2`. More specifically, `name` is the name of the volume, `path` is the directory location on host and `mountPath` is the mount path in container. |
| `remote-shuffle.kubernetes.worker.labels` | String | `''` | 1.0.0 | false | The user-specified labels to be set for the `ShuffleWorker` pods. Specified as key:value pairs separated by commas. For example, `version:alphav1,deploy:test`. |
| `remote-shuffle.kubernetes.worker.node-selector` | String | `''` | 1.0.0 | false | The user-specified node selector to be set for the `ShuffleWorker` pods. Specified as key:value pairs separated by commas. For example, `environment:production,disk:ssd`. |
| `remote-shuffle.kubernetes.worker.tolerations` | String | `''` | 1.0.0 | false | The user-specified tolerations to be set to the `ShuffleWorker` pods. The value should be in the form of `key:key1,operator:Equal,value:value1,effect:NoSchedule;key:key2,operator:Exists,effect:NoExecute,tolerationSeconds:6000`. |
| `remote-shuffle.kubernetes.manager.limit-factor.RESOURCE` | Integer | `1` | 1.0.0 | false | Kubernetes resource overuse limit factor for ShuffleManager. It should not be less than 1. The `RESOURCE` could be cpu, memory, ephemeral-storage and all other types supported by Kubernetes. For example, `remote-shuffle.kubernetes.manager.limit-factor.cpu: 8`. |
| `remote-shuffle.kubernetes.worker.limit-factor.RESOURCE` | Integer | `1` | 1.0.0 | false | Kubernetes resource overuse limit factor for `ShuffleWorker`. It should not be less than 1. The `RESOURCE` could be cpu, memory, ephemeral-storage and all other types supported by Kubernetes. For example, `remote-shuffle.kubernetes.manager.limit-factor.cpu: 8`. |
| `remote-shuffle.manager.memory.heap-size` | MemorySize | `4g` | 1.0.0 | false | Heap memory size to be used by the shuffle manager. |
| `remote-shuffle.manager.memory.off-heap-size` | MemorySize | `128m` | 1.0.0 | false | Off-heap memory size to be used by the shuffle manager. |
| `remote-shuffle.manager.jvm-opts` | String | `''` | 1.0.0 | false | Java options to start the JVM of the shuffle manager with. |
| `remote-shuffle.worker.memory.heap-size` | MemorySize | `1g` | 1.0.0 | false | Heap memory size to be used by the shuffle worker. |
| `remote-shuffle.worker.memory.off-heap-size` | MemorySize | `128m` | 1.0.0 | false | Off-heap memory size to be used by the shuffle worker. |
| `remote-shuffle.worker.jvm-opts` | String | `''` | 1.0.0 | false | Java options to start the JVM of the shuffle worker with. |

### Yarn Deployment Related

This section will present the valid config option that can be used by yarn deployment and should be
put in the yarn-site.xml file.

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.yarn.worker-stop-on-failure` | Bool | `false` | 1.0.0 | false | Flag indicating whether to throw the encountered exceptions to the upper Yarn service. The parameter's default value is false. If it is set as true, the upper Yarn service may be stopped because of the exceptions from the `ShuffleWorker`. Note: This parameter needs to be configured in yarn-site.xml. |

### Standalone Deployment Related

| Key | Value Type | Default Value | Version | Required | Description |
| --- | ---------- | ------------- | ------- | -------- | ----------- |
| `remote-shuffle.manager.memory.heap-size` | MemorySize | `4g` | 1.0.0 | false | Heap memory size to be used by the shuffle manager. |
| `remote-shuffle.manager.memory.off-heap-size` | MemorySize | `128m` | 1.0.0 | false | Off-heap memory size to be used by the shuffle manager. |
| `remote-shuffle.manager.jvm-opts` | String | `''` | 1.0.0 | false | Java options to start the JVM of the shuffle manager with. |
| `remote-shuffle.worker.memory.heap-size` | MemorySize | `1g` | 1.0.0 | false | Heap memory size to be used by the shuffle worker. |
| `remote-shuffle.worker.memory.off-heap-size` | MemorySize | `128m` | 1.0.0 | false | Off-heap memory size to be used by the shuffle worker. |
| `remote-shuffle.worker.jvm-opts` | String | `''` | 1.0.0 | false | Java options to start the JVM of the shuffle worker with. |

