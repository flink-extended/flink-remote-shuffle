# Remote Shuffle Service for Flink

- [Overview](#overview)
- [Supported Flink Version](#supported-flink-version)
- [Document](#document)
- [Building from Source](#building-from-source)
- [Example](#example)
- [How to Contribute](#how-to-contribute)
- [Support](#support)
- [Acknowledge](#acknowledge)

## Overview

This project implements a remote shuffle service for batch data processing
of [Flink](https://flink.apache.org/). By adopting the storage and compute separation architecture,
it brings several important benefits:

1. The scale up/down of computing resources and storage resources is now decoupled which means you
   can scale each up/down on demand freely.

2. Compute and storage stability never influence each other anymore. The remote shuffle service is
   free of user-code which can improve shuffle stability. For example, the termination
   of `TaskExecutor`s will not lead to data loss and the termination of remote `ShuffleWorker`s is
   tolerable.

3. By offloading the data shuffle work to the remote shuffle service, the computation resources can
   be released immediately after the upstream map tasks finish which can save resources.

In addition, the remote shuffle implementation borrows some good designs from Flink which can
benefit both stability and performance, for example:

1. Managed memory is preferred. Both the storage and network memory are managed which can
   significantly solve the OutOfMemory issue.

2. The credit-based backpressure mechanism is adopted which is good for both network stability and
   performance.

3. The zero-copy network data transmission is implemented which can save memory and is also good for
   stability and performance.

Besides, there are other important optimizations like load balancing and better sequential IO (
benefiting from the centralized service per node), tcp connection reuse, shuffle data compression,
adaptive execution (together with FLIP-187), etc.

Before going open source, this project has been used in production wildly and behaves well on both
stability and performance. Hope you enjoy it.

## Supported Flink Version

The remote shuffle service works together with Flink 1.14+. Some patches are needed to be applied to
Flink to support lower Flink versions. If you need any help on that, please let us know, we can
offer some help to prepare the patches for the Flink version you use.

## Document

The remote shuffle service supports standalone, yarn and k8s deployment. You can find the full user
guide [here](https://raw.githubusercontent.com/flink-extended/flink-remote-shuffle/master/docs/user_guide.md)
. In the future, more internal implementation detail specifications will be supplemented.

## Building from Source

To build this flink remote shuffle project from source, you should first clone the project:

```bash
git clone git@github.com:flink-extended/flink-remote-shuffle.git
```

Then you can build the project using maven (Maven and Java 8 required):

```bash
cd flink-remote-shuffle # switch to the remote shuffle project home directory
mvn package -DskipTests
```

After finish, you can find the target distribution in the build-target folder.

For k8s deployment, you can run the following command to build the docker image (Docker required):

```bash
cd flink-remote-shuffle # switch to the remote shuffle project home directory
sh ./tools/build_docker_image.sh
```

You can also publish the docker image by running the following command. The script that publishes
the docker image takes three arguments: the first one is the registry address (default value is
'docker.io'), the second one is the namespace (default value is 'flinkshuffle'), the third one is
the repository name (default value is 'flink-remote-shuffle').

```bash
cd flink-remote-shuffle # switch to the remote shuffle project home directory
sh ./tools/publish_docker_image.sh REGISTRY NAMESPACE REPOSITORY
```

## Example

After building the code from source, you can start and run a demo flink batch job using the remote
shuffle service locally (Flink 1.14+ required):

As the first step, you can download the Flink distribution from the
Flink's [download page](https://flink.apache.org/downloads.html#apache-flink-1140), for example,
Apache Flink 1.14.0 for Scala 2.11:

```bash
wget https://dlcdn.apache.org/flink/flink-1.14.0/flink-1.14.0-bin-scala_2.11.tgz
tar zxvf flink-1.14.0-bin-scala_2.11.tgz
```

Then after building the remote shuffle project from source, you can copy the shuffle plugin jar file
from build-target/lib directory (for example, build-target/lib/shuffle-plugin-0.3.0-SNAPSHOT.jar) to
the Flink lib directory and copy the build-in example job jar file to the flink home directory (
flink-1.14.0):

```bash
cp flink-remote-shuffle/build-target/lib/shuffle-plugin-0.3.0-SNAPSHOT.jar flink-1.14.0/lib/
cp flink-remote-shuffle/build-target/examples/BatchJobDemo.jar flink-1.14.0/
```

After that, you can start a local remote shuffle cluster by running the following command:

```bash
cd flink-remote-shuffle # switch to the remote shuffle project home directory
cd build-target # run after building from source
./bin/start-cluster.sh -D remote-shuffle.storage.local-data-dirs="[HDD]/tmp/" -D remote-shuffle.memory.data-writing-size=256m -D remote-shuffle.memory.data-reading-size=256m
```

Then you can start a local Flink cluster and config Flink to use the remote shuffle service by
running the following command:

```bash
cd flink-1.14.0 # switch to the flink home directory
./bin/start-cluster.sh -D shuffle-service-factory.class=com.alibaba.flink.shuffle.plugin.RemoteShuffleServiceFactory -D remote-shuffle.manager.rpc-address=127.0.0.1
```

Finally, you can run the demo batch job:

```bash
cd flink-1.14.0 # switch to the flink home directory
bin/flink run -c com.alibaba.flink.shuffle.examples.BatchJobDemo ./BatchJobDemo.jar
```

To stop the local cluster, you can just run the stop-cluster.sh script in the bin directory:

```bash
cd flink-1.14.0 # switch to the flink home directory
bin/stop-cluster.sh
```

```bash
cd flink-remote-shuffle # switch to the remote shuffle project home directory
bin/stop-cluster.sh
```

## How to Contribute

Any feedback of this project is highly appreciated. You can report a bug by opening an issue on
GitHub. You can also contribute any new features or improvements. See
the [contribution guide](https://raw.githubusercontent.com/flink-extended/flink-remote-shuffle/master/docs/contribution.md)
for more information.

## Support

We provide free support for users using this project. You can scan the following QR code to join
the [DingTalk](https://www.dingtalk.com/) user support group for further help and collaboration:

English:

<div align="center">
<img src="https://raw.githubusercontent.com/flink-extended/flink-remote-shuffle/master/docs/imgs/support-en.jpeg"/>
</div>

Chinese:

<div align="center">
<img src="https://raw.githubusercontent.com/flink-extended/flink-remote-shuffle/master/docs/imgs/support-zh.jpeg"/>
</div>

Another way is to join the Slack channel by clicking
this [invitation](https://join.slack.com/t/slack-5xu7894/shared_invite/zt-ykp807ok-1JXMcE6HS~NCplRp2T31fQ)
.

## Acknowledge

This is a Flink ecosystem project. Apache Flink is an excellent unified stateful data processing
engine. This project borrows some good designs (e.g. the credit-based backpressure) and building
blocks (e.g. rpc and high availability) from Flink.
