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

# Quick Start

- [Introduction](#introduction)
- [Build & Download](#build---download)
- [Browsing the Project Directory](#browsing-the-project-directory)
- [Starting Standalone Clusters](#starting-clusters)
  * [Starting a Standalone Remote Shuffle Cluster](#starting-a-standalone-remote-shuffle-cluster)
  * [Starting a Flink Cluster](#starting-a-flink-cluster)
- [Submitting a Flink Job](#submitting-a-flink-job)
- [Where to Go from Here](#where-to-go-from-here)


This tutorial provides a quick introduction to using remote shuffle service for Flink. This short guide will show you how to download the latest stable version of remote shuffle service, install and run it. You will also run an example Flink job using remote shuffle service and observe it in the Flink web UI.

## Introduction
Following the below steps in this quick start guide, you will run a simple Flink batch job which uses the remote shuffle service:
1. Download remote shuffle service and Flink binary releases.
2. Start a standalone remote shuffle service cluster.
3. Start a Flink local cluster that uses the remote shuffle service for data shuffling.
4. Submit a simple Flink batch job to the started cluster.

## Build & Download
Remote shuffler service runs on all UNIX-like environments, i.e. Linux, Mac OS X. You need to have Java 8 installed. To check the Java version installed, type in your terminal:

```sh
java -version
```

1. [Download the latest binary release](https://github.com/flink-extended/flink-remote-shuffle/releases) or [build remote shuffle service yourself](https://github.com/flink-extended/flink-remote-shuffle#building-from-source).
2. Next, [download the latest binary release](https://flink.apache.org/downloads.html) of Flink, then extract the archive:

```sh
 tar -xzf flink-*.tgz
```

For the steps of downloading or installing Flink, you can also refer to [Flink first steps](https://nightlies.apache.org/flink/flink-docs-release-1.14//docs/try-flink/local_installation/).

## Browsing the Project Directory
Navigate to the extracted shuffle service directory and list the contents by issuing:

```sh
cd flink-remote-shuffle* && ls -l
```

You should see some directories as follows.

| Directory | Meaning |
|--|--|
|`bin/` | Directory containing several bash scripts of the remote shuffle service that manage `ShuffleMananger` or `ShuffleWorker`.|
|`conf/` | Directory containing configuration files, including `remote-shuffle-conf.yaml`, `log4j2.properties`, etc.|
|`lib/` | Directory containing the remote shuffle service JARs compiled, including `shuffle-dist-*.jar`, log4j JARs, etc.|
|`log/` | Log directory should be empty. When running standalone shuffle service cluster, the logs of `ShuffleMananger` or `ShuffleWorker`s will be stored in this directory by default.|
|`opt/` | Directory containing the optional JARs used in some special environments, for example, `shuffle-kubernetes-operator-*.jar` is used when deploying on Kubernetes.|
|`examples/` | Directory containing several demo example JARs. |

## Starting Clusters
### Starting a Standalone Remote Shuffle Cluster
Please refer to [how to start a standalone remote shuffle cluster](https://github.com/flink-extended/flink-remote-shuffle/blob/master/docs/deploy_standalone_mode.md#cluster-quick-start-script).

### Starting a Flink Cluster
Before starting a Flink local cluster,
1. Make sure that a valid remote shuffle service cluster has been successfully started.
2. You need to copy the shuffle plugin JAR from the remote shuffle `lib` directory (for example, `lib/shuffle-plugin-*.jar`) to the Flink `lib` directory.

For different startup modes of remote shuffle service, Flink job configurations are different, the details are as follows.

- For standalone remote shuffle service, please add the following configurations to `conf/flink-conf.yaml` in the extracted Flink directory to use remote shuffle service when running a Flink batch job. The argument `manager-ip-address` is the ip address of `ShuffleManager` (for local remote shuffle cluster, it should be 127.0.0.1). 

```yaml
shuffle-service-factory.class: com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory
remote-shuffle.manager.rpc-address: <manager-ip-address>
```

- For remote shuffle service on YARN or Kubernetes, please add the following configurations to `conf/flink-conf.yaml` in the extracted Flink directory to use remote shuffle service when running a Flink batch job. `remote-shuffle.ha.zookeeper.quorum` is the Zookeeper address of the `ShuffleManager` when high availability is enabled.

```yaml
shuffle-service-factory.class: com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory
remote-shuffle.high-availability.mode: ZOOKEEPER
remote-shuffle.ha.zookeeper.quorum: zk1.host:2181,zk2.host:2181,zk3.host:2181
```

Please refer the following links for different deployment mode of Flink:
- For standalone Flink cluster, please refer to [Flink standalone mode](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/).
- For Flink cluster on YARN, please refer to [Flink on YARN](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/yarn/).
- For Flink cluster on YARN, please refer to [Flink on Kubernetes](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/kubernetes/) or [natively on Kubernetes](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/native_kubernetes/).

Usually, starting a local Flink cluster by running the following command is enough for this quick start guide:

```sh
# We assume to be in the root directory of the Flink extracted distribution

./bin/start-cluster.sh
```

You should be able to navigate to the web UI at `http://<job manager ip address>:8081` to view the Flink dashboard and see that the cluster is up and running.

Because the configurations related to shuffle have been modified, all jobs submitted to the Flink cluster will use remote shuffle service to shuffle data.

## Submitting a Flink Job

After starting the Flink cluster successfully, you can submit a simple Flink batch demo job.

The example source code is in the `shuffle-examples` module. `BatchJobDemo` is a simple Flink batch job. And you need to copy the compiled demo JAR `examples/BatchJobDemo.jar` to the extracted Flink directory. Please run the following command to submit the example batch job.

```sh
# Firstly, copy the example JAR
# cp examples/BatchJobDemo.jar <Flink directory>

# We assume to be in the root directory of the Flink extracted distribution

./bin/flink run ./BatchJobDemo.jar
```

You have successfully ran a Flink batch job using remote shuffle service.

## Where to Go from Here
Congratulations on running your first Flink application using remote shuffle service!

- For an in-depth overview of the remote shuffle service, start with the [user guide](https://github.com/flink-extended/flink-remote-shuffle/blob/master/docs/user_guid.md).
- For running applications on a cluster, head to the [deployment overview](https://github.com/flink-extended/flink-remote-shuffle/blob/master/docs/user_guid.md#deployment).
- For detailed configurations, refer to the [configuration page](https://github.com/flink-extended/flink-remote-shuffle/blob/master/docs/configuration.md).
