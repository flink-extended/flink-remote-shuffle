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

# Running Remote Shuffle Service on YARN

- [Getting Started](#getting-started)
	- [Introduction](#introduction)
	- [Preparation](#preparation)
	- [Starting a ShuffleManager on YARN](#starting-a-shufflemanager-on-yarn)
	- [Starting ShuffleWorkers on YARN](#starting-shuffleworkers-on-yarn)
- [Submitting a Flink Job](#submitting-a-flink-job)
- [Logging](#logging)
- [Supported Hadoop versions](#supported-hadoop-versions)

## Getting Started
This *Getting Started* section guides you through setting up a fully functional Flink remote shuffle service on YARN.

### Introduction
[YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html) is a very popular resource management framework. Remote shuffle service for Flink can be deployed on YARN. `ShuffleManager` and `ShuffleWorker`s are started on YARN framework in different ways. `ShuffleManager` runs in the ApplicationMaster container of a special YARN application. `ShuffleWorker`s run as [Auxiliary Services](https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/PluggableShuffleAndPluggableSort.html) on `NodeManager`s.

### Preparation
This *Getting Started* section assumes a functional YARN environment (>= 2.4.1). YARN environments are provided most conveniently through services such as Alibaba Cloud, Google Cloud DataProc, Amazon EMR, or products like Cloudera, etc. You can also refer to [setting up a YARN environment locally](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html) or [setting up a YARN cluster](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html) to setup a YARN environment manually.

In addition, Zookeeper cluster is used for high availability. When the manager is started on the YARN container, its address information is written to Zookeeper, from where the `ShuffleWorker`s and `ShuffleClient` can obtain the manager's address.

So the prerequisites to deploy remote shuffle service on YARN are as follows.

- Make sure your YARN cluster is ready for accepting YARN applications.
- [Download the latest binary release](https://github.com/flink-extended/flink-remote-shuffle/releases) or [build remote shuffle service yourself](https://github.com/flink-extended/flink-remote-shuffle#building-from-source).
- **Important** Make sure `HADOOP_YARN_HOME` environment variable is set up. Use the command `yarn` or `$HADOOP_YARN_HOME/bin/yarn` to check whether the environment variable is set successfully, and the output should not display any errors.
- Make sure a valid Zookeeper cluster is ready. Or you can refer to [setting up a Zookeeper cluster](https://zookeeper.apache.org/doc/current/zookeeperStarted.html) to start a Zookeeper cluster manually.

### Starting a ShuffleManager on YARN
Once the above prerequisites are ready, you can start `ShuffleManager` on YARN:

```sh
# We assume to be in the root directory of the unzipped distribution

# Start a ShuffleManager on YARN
./bin/yarn-shufflemanager.sh start --am-mem-mb 4096 -q root.default -D remote-shuffle.high-availability.mode=ZOOKEEPER -D remote-shuffle.ha.zookeeper.quorum=zk1.host:2181,zk2.host:2181,zk3.host:2181

# Stop the ShuffleManager on YARN
./bin/yarn-shufflemanager.sh stop

# Use the following command to display detailed usage.
./bin/yarn-shufflemanager.sh -h
```

`ShuffleManager` can be configured through the `./bin/yarn-shufflemanager.sh` script using dynamic parameters. Any configurations in [configuration page](./configuration.md) supported by remote shuffle service can be passed in as a parameter of the script, such as `-D <config key>=<config value>`. You can also put these configurations in `conf/remote-shuffle-conf.yaml`.

Through querying the metric server or checking the container output log, you can **check whether the manager is started** successfully. The default metric server address is `http://<manager-ip-address>:23101/metrics/`.

You have successfully start a `ShuffleManager` on YARN. If any exception occurs, the current`ShuffleManager` may stop and another new `ShuffleManager` will start in a new container, which will not affect the runing Flink batch jobs.

After `ShuffleManager` is started, you can start `ShuffleWorker`s on `NodeManager`s, which will register to the started `ShuffleManager` based on the address obtained from the Zookeeper.

### Starting ShuffleWorkers on YARN
Each `ShuffleWorker` starts as an auxiliary service on `NodeManager`.
To start the `ShuffleWorker` on each `NodaManager`, follow these instructions:

1. Locate the `shuffle-dist-<version>.jar`. If you compile the project manually, this should be under `./build-target/lib/` directory. If you use the download distribution, this should be under `./lib/` directory.

2. Add the `shuffle-dist-<version>.jar` to the classpath of each `NodeManager` in your YARN cluster. To achieve this, you can either add the following command to `<hadoop home dir>/conf/yarn-env.sh` or move the `shuffle-dist-<version>.jar` to `<hadoop home dir>/share/hadoop/yarn/`.

```sh
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:<the path that contains the JAR>/shuffle-dist-<version>.jar
```

3. In the `etc/hadoop/yarn-site.xml` on each `NodeManager`, add the following configurations. Please note that the Zookeeper address `remote-shuffle.ha.zookeeper.quorum` should be the same as the address used by the `ShuffleManager` at startup. `remote-shuffle.storage.local-data-dirs` is the local file system directories to persist partitioned data to. In order to avoid the death of `ShuffleManager` due to timeout when `ShuffleManager` does not exist, set the timeout to a very large value by `remote-shuffle.cluster.registration.timeout`, for example, set to 1 year. Any configurations in [configuration page](./configuration.md) can be added to **`yarn-site.xml`** to control the behavior of `ShuffleWorker`.

```xml
    <property>
      <name>remote-shuffle.high-availability.mode</name>
      <value>ZOOKEEPER</value>
    </property>

    <property>
      <name>remote-shuffle.ha.zookeeper.quorum</name>
      <value>zk1.host:2181,zk2.host:2181,zk3.host:2181</value>
    </property>

    <property>
      <name>remote-shuffle.storage.local-data-dirs</name>
      <!-- The prefix [HDD] and [SSD] indicate the disk type. -->
      <value>[SSD]/PATH1/rss_data_dir1/,[HDD]/PATH2/rss_data_dir2/</value>
    </property>

    <!-- Set to a very large value to avoid timeout. -->
    <property>
      <name>remote-shuffle.cluster.registration.timeout</name>
      <value>31536000000</value>
    </property>
```

4. Restart all `NodeManager`s in your cluster. Note that the heap and direct memory size of `NodeManager` should be increased to avoid `out of memory` problems. The heap size of `ShuffleWorker` is mainly used by `remote-shuffle.worker.memory.heap-size`, which is 1g by default. The direct memory used includes `remote-shuffle.worker.memory.off-heap-size`(128m by default), `remote-shuffle.memory.data-writing-size`(4g by default) and `remote-shuffle.memory.data-reading-size`(4g by default). In total, please increase at least 1g heap size and 8.2g direct memory size by default for `NodeManager`. In your production environment, you can adjust these configurations to change the memory usage of `ShuffleWorker`.

Alternatively, when starting a local standalone or YARN cluster on your laptop, you can reduce `remote-shuffle.memory.data-reading-size` or `remote-shuffle.memory.data-writing-size` to decrease the memory usage of `ShuffleWorker`, for example, set to 128m. For more `ShuffleWorker` configurations, please refer to [configuration page](./configuration.md).

Through querying the metric server or checking the container output log, you can **check whether a worker is started** successfully. The default metric server address is `http://<worker-ip-address>:23103/metrics/` by default.

Now you have started a remote shuffle service cluster on YARN successfully.

## Submitting a Flink Job

To submit a Flink job, please refer to [starting a Flink cluster](./quick_start.md#starting-a-flink-cluster) and [submitting a Flink job](./quick_start.md#submitting-a-flink-job).

## Logging
**ShuffleManager Log**

For `ShuffleManager` running on YARN, the log is output to the container log. You can use `conf/log4j2.properties` to modify the log level, log output format, etc.

To enable GC log or modify other JVM GC options for `ShuffleManager`, add `remote-shuffle.yarn.manager-am-jvm-options` in `conf/remote-shuffle-conf.yaml`,
the following is a simple example:

```yaml
remote-shuffle.yarn.manager-am-jvm-options: -verbose:gc -XX:NewRatio=3 -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:ParallelGCThreads=4 -XX:+UseGCLogFileRotation
```

**ShuffleWorker Log**

For `ShuffleWorker` running on YARN, as an auxiliary service of `NodeManager`, its log is output to the log of `NodeManager` by default.

If you want to separate its logs from `NodeManager` logs by directory or modify the log level, you can modify `conf/log4j.properties` in **Hadoop** directory and restart all `NodeManager`s. Here is an example.

```properties
# Flink Remote Shuffle Service Logs
flink.shuffle.logger=INFO,FLINKRSS
flink.shuffle.log.maxfilesize=512MB
flink.shuffle.log.maxbackupindex=20
log4j.logger.com.alibaba.flink.shuffle=${flink.shuffle.logger}
log4j.additivity.com.alibaba.flink.shuffle=false
log4j.appender.FLINKRSS=org.apache.log4j.RollingFileAppender
log4j.appender.FLINKRSS.File=/flink-remote-shuffle/log/flink-remote-shuffle.log
log4j.appender.FLINKRSS.layout=org.apache.log4j.PatternLayout
log4j.appender.FLINKRSS.layout.ConversionPattern=%d{ISO8601} %p %c{2}: %m%n
log4j.appender.FLINKRSS.MaxFileSize=${flink.shuffle.log.maxfilesize}
log4j.appender.FLINKRSS.MaxBackupIndex=${flink.shuffle.log.maxbackupindex}
```

In this way, `ShuffleWorker` logs will be separated from `NodeManager` logs. All logs of the classes starting with `com.alibaba.flink.shuffle` will be output to `/flink-remote-shuffle/log/flink-remote-shuffle.log`.

## Supported Hadoop versions
Remote shuffle service for Flink on YARN is compiled against Hadoop 2.4.1, and all Hadoop versions >= 2.4.1 are supported, including Hadoop 3.x.

