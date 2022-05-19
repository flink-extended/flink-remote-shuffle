<!--
 Copyright 2021 The Flink Remote Shuffle Project

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Remote Shuffle Service Standalone Mode

- [Introduction](#introduction)
- [Preparation](#preparation)
- [Cluster Quick Start Script](#cluster-quick-start-script)
- [Single Component Management Script](#single-component-management-script)
- [Submitting a Flink Job](#submitting-a-flink-job)
- [Logging & JVM arguments](#logging--jvm-arguments)

## Introduction
In addition to running on the YARN and Kubernetes cluster managers, remote shuffle service also provides a simple standalone deploy mode. You have to take care of restarting failed processes and resources allocation during operation in this mode.

This page mainly introduces two methods to start a standalone cluster:
1. Start a cluster with management scripts. With this method, cluster management becomes easier. After simple configuration, you can easily start `ShuffleManager` and `ShuffleWorker` with one line of command.
2. Start `ShuffleManager` and `ShuffleWorker` one by one with separate commands, which facilitates independent management of individual components.

The followings are two important config options to be used:

| Argument | Meaning |
| -------- | ------- |
|`remote-shuffle.manager.rpc-address` | The network IP address to connect to for communication with the shuffle manager. Only IP address without port.|
|`remote-shuffle.storage.local-data-dirs` | Local file system directories to persist partitioned data to. Multiple directories can be configured and these directories should be separated by comma ','. For example, [SSD]/PATH1/rss_data_dir1/,[HDD]/PATH2/rss_data_dir2/, the prefix [HDD] and [SSD] indicate the disk type.|

## Preparation
Remote shuffle service runs on all UNIX-like environments, e.g. Linux, Mac OS X. Before you start the standalone cluster, make sure your system fullfils the following requirements.

- Java 1.8.x or higher installed,
- [Download the latest binary release](https://github.com/flink-extended/flink-remote-shuffle/releases) or [build remote shuffle service yourself](https://github.com/flink-extended/flink-remote-shuffle#building-from-source).

## Cluster Quick Start Script
**Starting and Stopping a cluster**

`bin/start-cluster.sh` and `bin/stop-cluster.sh` rely on `conf/workers` to determine the number of cluster component instances. Note that you should only start one `ShuffleWorker` instance per physical node.

If password-less SSH access to the listed machines is configured, and they share the same directory structure, the scripts can support starting and stopping instances remotely.

***Example: Start a distributed shuffle service cluster with 2 ShuffleWorkers***

At present, only one manager is supported in standalone mode. 

Contents of `conf/managers`:

```
manager1
```

`manager1` is the **actual IP address** where the `ShuffleManager` is started. Only one address is required. If multiple addresses are filled in, the first will be used. The `ShuffleManager` RPC port can be configured by `remote-shuffle.manager.rpc-port` in `conf/remote-shuffle-conf.yaml`, if not configured, port 23123 will be used by default.

Note that if you want to start `ShuffleWorker`s on multiple machines, you need to replace `127.0.0.1` in `conf/managers` with the actual IP address. Otherwise, workers started on other machines cannot get the right `ShuffleManager` IP address and cannot connect to the manager.

If you want to start only one `ShuffleWorker` and the `ShuffleWorker` is on the same machine where the `ShuffleManager` is started, the default `127.0.0.1` in `conf/managers` can meet the requirements.

Contents of `conf/workers`:

```
worker1
worker2
```

Note that two workers means that at least two physical nodes are needed, if you only have one physical node, please only configure one worker here.

Then you can execute the following command to start the cluster:

```sh
./bin/start-cluster.sh -D remote-shuffle.storage.local-data-dirs="[SSD]/PATH/rss_data_dir/"
```

When executing `bin/start-cluster.sh`, please make sure the following requirements are ready.
- Password-less SSH access to the listed machines is configured.
- The same directory structure of the remote shuffle distribution should exist on each listed machine.
- A shuffle data directory(`[HDD]/PATH/rss_data_dir/`) on each `ShuffleWorker` should be created and the directory permissions should be accessible.

You can use "-D" to pass any configuration options in [configuration page](./configuration.md) as parameters of `./bin/start-cluster.sh` to control the behavior of `ShuffleManager` and  `ShuffleWorker`s, for example, `-D <config key>=<config value>`. These configurations can also be configured in `conf/remote-shuffle-conf.yaml`.

After running `bin/start-cluster.sh`, the output log is as follows.

```sh
Starting cluster.
Starting shufflemanager daemon on host your-host.
Starting shuffleworker daemon on host your-host.
```

If `bin/start-cluster.sh` is executed successfully, you have started a remote shuffle service cluster with a manager and 2 workers.
The `ShuffleManager` log and `ShuffleWorker` log will be output to the `log/` directory by default.

You can stop the cluster with the command:

```sh
./bin/stop-cluster.sh
```

The above illustrates a convenient way to manage a cluster.

## Single Component Management Script

This section describes another way to start a cluster.

**Starting a ShuffleManager**

You can start a standalone `ShuffleManager` by executing:

```sh
./shufflemanager.sh start -D remote-shuffle.manager.rpc-address=<manager-ip-address>
```

Note that `manager-ip-address` is the real address that can be connected from the outside which is not `127.0.0.1` or `localhost`.

You can use "-D" to pass all options in the [configuration page](./configuration.md) to `ShuffleManager`, for example, `-D <config key>=<config value>`. These configurations can also be configured in `conf/remote-shuffle-conf.yaml`.

Through querying the metric server or checking the container output log, you can **check whether the manager is started** successfully. The default metric server address is `http://<manager-ip-address>:23101/metrics/`.

Before starting a `ShuffleWorker`, you need create a directory to store shuffle data files, which is an indispensable configuration option. For example, the  directory created is `[SSD]/PATH/rss_data_dir/`, which is a SSD type disk.

**Starting ShuffleWorkers**

Similarly, you can start one or more workers and connect them to the manager via:

```sh
./bin/shuffleworker.sh start -D remote-shuffle.manager.rpc-address=<manager-ip-address> -D remote-shuffle.storage.local-data-dirs="[HDD]/PATH/rss_data_dir/"
```

You can use "-D" to pass all options in the [configuration page](./configuration.md) to `ShuffleWorker`, for example, `-D <config key>=<config value>`. These configurations can also be configured in `conf/remote-shuffle-conf.yaml`.

Through querying the metric server or checking the container output log, you can **check whether a worker is started** successfully. The default metric server address is `http://<worker-ip-address>:23103/metrics/`.

## Submitting a Flink Job
To submit a Flink job, please refer to [starting a Flink cluster](./quick_start.md#starting-a-flink-cluster) and [submitting a Flink job](./quick_start.md#submitting-a-flink-job).

## Logging & JVM Arguments
**Configuring Log4j**

You can modify `log4j2.properties` to control the log output format, log level, etc. For example, change `rootLogger.level=INFO` to `rootLogger.level=DEBUG` to enable debug logging.

**Configuring Log Options and JVM Arguments**

Adding configuration options to `conf/remote-shuffle-conf.yaml` to configure the log directory, memory size, JVM GC log, JVM GC options, etc. Here is a general example.

```yaml
remote-shuffle.manager.jvm-opts: -verbose:gc -Dlog.file=/flink-remote-shuffle/log/shufflemanager.log -Xloggc:/flink-remote-shuffle/log/shufflemanager.gc.log -XX:NewRatio=3 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4 -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=256M

remote-shuffle.worker.jvm-opts: -verbose:gc -Dlog.file=/flink-remote-shuffle/log/shuffleworker.log -Xloggc:/flink-remote-shuffle/log/shuffleworker.gc.log -XX:NewRatio=3 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4 -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=2 -XX:GCLogFileSize=256M
```

According to this example configuration, Logs and GC logs will be output to `/flink-remote-shuffle/log/`. By default, no GC logs will be output and the logs are in `<home dir of shuffle service>/log`. Please modify or add new parameters to control log output and JVM GC according to your production environment.

