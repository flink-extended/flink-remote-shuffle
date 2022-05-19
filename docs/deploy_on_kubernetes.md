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

# Running Remote Shuffle Service on Kubernetes

- [Getting Started](#getting-started)
	- [Introduction](#introduction)
	- [Preparation](#preparation)
- [Deploying Remote Shuffle Service Cluster](#deploying-remote-shuffle-service-cluster)
	- [Deploying Remote Shuffle Operator](#deploying-remote-shuffle-operator)
	- [Deploying Remote Shuffle Cluster](#deploying-remote-shuffle-cluster)
- [Submitting a Flink Job](#submitting-a-flink-job)
- [Logging & Configuration](#logging--configuration)

## Getting Started
This page describes how to deploy remote shuffle service on Kubernetes. You can use the released image directly: docker.io/flinkremoteshuffle/flink-remote-shuffle:VERSION. Note that you need to replace the 'VERSION' filed with the actual version you want to use, for example, 1.0.0.

### Introduction
Kubernetes is a popular container-orchestration system for automating application deployment, scaling, and management. Remote shuffle service allows you to directly deploy the services on a running Kubernetes cluster.

### Preparation
The `Getting Started` section assumes that your environment fulfills the following requirements:
- A functional Kubernetes cluster (Kubernetes >= 1.13).

- Make sure a valid Zookeeper cluster is ready. Or you can refer to [setting up a Zookeeper cluster](https://zookeeper.apache.org/doc/current/zookeeperStarted.html) to start a Zookeeper cluster manually.

- [Download the latest binary release](https://github.com/flink-extended/flink-remote-shuffle/releases) or [build remote shuffle service yourself](https://github.com/flink-extended/flink-remote-shuffle#building-from-source).

If you have problems setting up a Kubernetes cluster, take a look at [how to setup a Kubernetes cluster](https://kubernetes.io/docs/setup/).

## Deploying Remote Shuffle Service Cluster
The remote shuffle service cluster contains a `ShuffleManager` and multiple `ShuffleWorker`s. The `ShuffleManager` runs as a Kubernetes [Deployment](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) (the number of replicas is 1), and the shuffle workers run as a Kubernetes [DaemonSet](https://kubernetes.io/docs/concepts/workloads/controllers/daemonset/) which means the number of `ShuffleWorker`s is the same as the number of machines in the Kubernetes cluster. The following two points should be noted here:

1. Currently, we only support host network for network communication.

2. The shuffle workers use a [hostPath](https://kubernetes.io/docs/concepts/storage/volumes/#hostpath) volume (specified by `remote-shuffle.kubernetes.worker.volume.host-paths`) or a [emptydir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) volume (specified by `remote-shuffle.kubernetes.worker.volume.empty-dirs`) for shuffle data storage.

Additionally, to make it easier to deploy on a Kubernetes cluster, we provided a Kubernetes [Operator](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/) for remote shuffle service, which can control the life cycle of remote shuffle service instances, including creation, deletion, and upgrade.

### Deploying Remote Shuffle Operator
Once you have your Kubernetes cluster ready and `kubectl` is configured to point to it, you can launch an operator via:

```sh
# Note: You must configure the docker image to be used by modifying the template file first before running this command.

kubectl apply -f kubernetes-shuffle-operator-template.yaml
```

The template file `kubernetes-shuffle-operator-template.yaml` should be in `conf/` directory and its contents are as follows.

```yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: flink-rss-cr
rules:
- apiGroups: ["apiextensions.k8s.io"]
  resources:
  - customresourcedefinitions
  verbs:
  - '*'
- apiGroups: ["shuffleoperator.alibaba.com"]
  resources:
  - remoteshuffles
  verbs:
  - '*'
- apiGroups: ["shuffleoperator.alibaba.com"]
  resources:
  - remoteshuffles/status
  verbs:
  - update
- apiGroups: ["apps"]
  resources:
  - deployments
  - daemonsets
  verbs:
  - '*'
- apiGroups: [""]
  resources:
  - configmaps
  verbs:
  - '*'
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: flink-rss-crb
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: flink-rss-cr
subjects:
- kind: ServiceAccount
  name: flink-rss-sa
  namespace: flink-system-rss
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: flink-rss-sa
  namespace: flink-system-rss
---
apiVersion: apps/v1
kind: Deployment
metadata:
  namespace: flink-system-rss
  name: flink-remote-shuffle-operator
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink-remote-shuffle-operator
  template:
    metadata:
      labels:
        app: flink-remote-shuffle-operator
    spec:
      serviceAccountName: flink-rss-sa
      containers:
        - name: flink-remote-shuffle-operator
          image: <docker image> # You need to configure the docker image to be used here.
          imagePullPolicy: Always
          command:
            - bash
          args:
            - -c
            - $JAVA_HOME/bin/java -classpath '/flink-remote-shuffle/opt/*' -Dlog4j.configurationFile=file:/flink-remote-shuffle/conf/log4j2-operator.properties -Dlog.file=/flink-remote-shuffle/log/operator.log com.alibaba.flink.shuffle.kubernetes.operator.RemoteShuffleApplicationOperatorEntrypoint
```

### Deploying Remote Shuffle Cluster
Then you can start `ShuffleManager` and `ShuffleWorker`s via:

```sh
# Note: You must accomplish the template file first before running this command.

kubectl apply -f kubernetes-shuffle-cluster-template.yaml
```

The template file `kubernetes-shuffle-cluster-template.yaml` should be in `conf/` directory and its contents are as follows.

```yaml
apiVersion: shuffleoperator.alibaba.com/v1
kind: RemoteShuffle
metadata:
  namespace: flink-system-rss
  name: flink-remote-shuffle
spec:
  shuffleDynamicConfigs:
    remote-shuffle.manager.jvm-opts: -verbose:gc -Xloggc:/flink-remote-shuffle/log/gc.log
    remote-shuffle.worker.jvm-opts: -verbose:gc -Xloggc:/flink-remote-shuffle/log/gc.log
    remote-shuffle.kubernetes.manager.cpu: 4
    remote-shuffle.kubernetes.worker.cpu: 4
    remote-shuffle.kubernetes.worker.limit-factor.cpu: 8
    remote-shuffle.kubernetes.container.image: <docker image>
    remote-shuffle.kubernetes.worker.volume.host-paths: name:disk,path:<dir on host>,mountPath:/data
    remote-shuffle.storage.local-data-dirs: '[SSD]/data'
    remote-shuffle.high-availability.mode: ZOOKEEPER
    remote-shuffle.ha.zookeeper.quorum: <zookeeper quorum>
    remote-shuffle.kubernetes.manager.env-vars: <env-vars> # You need to configure your time zone here, for example, TZ:Asia/Shanghai.
    remote-shuffle.kubernetes.worker.env-vars:  <env-vars> # You need to configure your time zone here, for example, TZ:Asia/Shanghai.

  shuffleFileConfigs:
    log4j2.properties: |
      monitorInterval=30

      rootLogger.level = INFO
      rootLogger.appenderRef.console.ref = ConsoleAppender
      rootLogger.appenderRef.rolling.ref = RollingFileAppender

      # Log all info to the console
      appender.console.name = ConsoleAppender
      appender.console.type = CONSOLE
      appender.console.layout.type = PatternLayout
      appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%t] %-60c %x - %m%n

      # Log all info in the given rolling file
      appender.rolling.name = RollingFileAppender
      appender.rolling.type = RollingFile
      appender.rolling.append = true
      appender.rolling.fileName = ${sys:log.file}
      appender.rolling.filePattern = ${sys:log.file}.%i
      appender.rolling.layout.type = PatternLayout
      appender.rolling.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%t] %-60c %x - %m%n
      appender.rolling.policies.type = Policies
      appender.rolling.policies.size.type = SizeBasedTriggeringPolicy
      appender.rolling.policies.size.size=256MB
      appender.rolling.policies.startup.type = OnStartupTriggeringPolicy
      appender.rolling.strategy.type = DefaultRolloverStrategy
      appender.rolling.strategy.max = ${env:MAX_LOG_FILE_NUMBER:-10}
```

- Note that `remote-shuffle.ha.zookeeper.quorum` should be accomplished according to the actual environment.

- Note that `remote-shuffle.kubernetes.container.image` should be accomplished according to the shuffle service image built from source code.

- Note that `remote-shuffle.kubernetes.worker.volume.host-paths` should be accomplished according to the actual storage path on host to be used.

- Note that `remote-shuffle.kubernetes.manager.env-vars` and `remote-shuffle.kubernetes.worker.env-vars` should be accomplished to specify the right time zone.

If you want to build a new image by yourself, please refer to [preparing docker environment](https://docs.docker.com/get-docker/) and [building from source](https://github.com/flink-extended/flink-remote-shuffle#building-from-source).

After successfully running the above command `kubectl apply -f XXX`, a new shuffle service cluster will be started.

## Submitting a Flink Job

To submit a Flink job, please refer to [starting a Flink cluster](./quick_start.md#starting-a-flink-cluster) and [submitting a Flink job](./quick_start.md#submitting-a-flink-job).

If you would like to run Flink jobs on Kubernetes, you need to follow the below steps:

1. First of all, you need to build a new Flink docker image which contains remote shuffle plugin JAR file. Please refer to [how to customize the Flink Docker image](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/docker/#advanced-customization) for more information. The following is a simple customized Flink Docker file example:

```dockerfile
FROM flink

# The path of shuffle plugin JAR should be the lib/ directory of the remote shuffle distribution which need to be replaced by the really path in your environment.
COPY /<Path of the shuffle plugin JAR>/shuffle-plugin-<version>.jar /opt/flink/lib/
```

2. The you should add the following configurations to `conf/flink-conf.yaml` in the extracted Flink directory to configure Flink to use the remote shuffle service. Please note that the configuration of `remote-shuffle.ha.zookeeper.quorum` should be exactly the same as that in `kubernetes-shuffle-cluster-template.yaml`.

```yaml
shuffle-service-factory.class: com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory
remote-shuffle.high-availability.mode: ZOOKEEPER
remote-shuffle.ha.zookeeper.quorum: zk1.host:2181,zk2.host:2181,zk3.host:2181
```

3. Finally, you can start a Flink cluster on Kubernetes and submit a Flink job. Please refer to [start a Flink cluster on Kubernetes](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/kubernetes/) or [Flink natively on Kubernetes](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/native_kubernetes/) for more information.

## Logging & Configuration

From the above YAML file templates, you can figure out how to configure remote shuffle service on Kubernetes. 

Kubernetes operator related options and log output file are specified in `kubernetes-shuffle-operator-template.yaml`.

Any configurations in [configuration page](./configuration.md), log output format and log appender options of `ShuffleManager` and `ShuffleWorker` are configured in `kubernetes-shuffle-cluster-template.yaml`.

