#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Start a ShuffleManager on Yarn.

usage() {
    echo "
Usage:
  start                 start shuffle manager on Yarn.
  stop                  stop shuffle manager on Yarn.
                            If the applicatioin name is not specified, the default shufflemanager with the
                            name 'Flink-Remote-Shuffle-Manager' will be killed.
  restart               resart shuffle manager on Yarn.

  Optional args:
  <-q, --queue>         queue name to start shuffle manager, default queue is 'default'
  <-n, --name>          application name, the name should not contain spaces
  <-c, --clusterid>     the cluster id to start shuffle manager, default cluster id is 'default-cluster'
  <-p, --priority>      application priority when deploying shuffle manager service on Yarn, default is 0
  <-a, --attempts>      application master max attempt counts, default is 1000000
  <--am-mem-mb>         size of memory in megabytes allocated for starting shuffle manager, default is 2048
  <--am-overhead-mb>    size of overhead memory in megabytes allocated for starting shuffle manager, default is 512
  <--rm-heartbeat-ms>   heartbeat interval between shuffle manager am and yarn resource manager, default is 1000
  <--check-old-job>     flag indicating whether to check old job is exist, default is False, set to True to enable it
  -h, --help            display this help and exit

  example1: yarn-shufflemanager.sh start --queue default --am-mem-mb 1024
  example2: yarn-shufflemanager.sh start -q default -D remote-shuffle.xxx=xxx
  example3: yarn-shufflemanager.sh stop
"
}

JAR_NAME="shuffle-dist"
RUN_CLASS="com.alibaba.flink.shuffle.yarn.entry.manager.YarnShuffleManagerEntrypoint"

confDir="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
homeDir=$(dirname $confDir)
libDir=$homeDir/lib
logDir=$homeDir/log
runCmd=""
jarFile=""
cmdType=""

# Set default values for all args
yarnCmd=yarn
queue=default
name="Flink-Remote-Shuffle-Manager"
priority=0
attempts=1000000
memSize=2048
memOverheadSize=512
heartbeatInterval=1000
checkOldJobExist=False
clusterid=default-cluster

parseArgs() {
    runCmd=$runCmd" -D remote-shuffle.yarn.manager-home-dir=$homeDir"
    while [[ $# -gt 0 ]]; do
        case "$1" in
        start | stop | restart)
            cmdType="$1"
            ;;
        -q | --queue)
            queue="$2"
            echo "arg, queue:             $queue"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-app-queue-name=$queue"
            shift
            ;;
        -n | --name)
            name="$2"
            splitNames=(${name// / })
            if [[ ${#splitNames[*]} -gt 1 ]]; then
                echo "The specified name by -n or --name can not contain spaces."
                exit 1
            fi
            echo "arg, name:              $name"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-app-name=$name"
            shift
            ;;
        -c | --clusterid)
            clusterid="$2"
            echo "arg, clusterid:         $clusterid"
            runCmd=$runCmd" -D remote-shuffle.cluster.id=$clusterid"
            shift
            ;;
        -p | --priority)
            priority="$2"
            echo "arg, priority:          $priority"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-app-priority=$priority"
            shift
            ;;
        -a | --attempts)
            attempts="$2"
            echo "arg, attempts:          $attempts"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-am-max-attempts=$attempts"
            shift
            ;;
        --am-mem-mb)
            memSize="$2"
            echo "arg, memSize:           $memSize"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-am-memory-size-mb=$memSize"
            shift
            ;;
        --am-overhead-mb)
            memOverheadSize="$2"
            echo "arg, memOverheadSize:   $memOverheadSize"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-am-memory-overhead-mb=$memOverheadSize"
            shift
            ;;
        --rm-heartbeat-ms)
            heartbeatInterval="$2"
            echo "arg, heartbeatInterval: $heartbeatInterval"
            runCmd=$runCmd" -D remote-shuffle.yarn.manager-rm-heartbeat-interval-ms=$heartbeatInterval"
            shift
            ;;
        --check-old-job)
            checkOldJobExist="$2"
            echo "arg, checkOldJobExist:    $checkOldJobExist"
            shift
            ;;
        -D)
            runCmd=$runCmd" -D $2"
            echo "arg, internal arg:      $2"
            shift
            ;;
        -h | --help)
            usage
            exit
            ;;
        --)
            shift
            break
            ;;
        *)
            echo "$1 is not a valid option"
            exit 1
            ;;
        esac
        shift
    done

    if [[ $cmdType != "" ]]; then
        echo
        echo "cmd type:   $cmdType"
        echo
    fi
}

checkYarnEnv() {
    isYarnExist=false
    if [[ -x "$(command -v yarn)" ]]; then
        isYarnExist=true
        yarnCmd="yarn"
    fi

    if [[ $HADOOP_YARN_HOME != "" && -x "$(command -v $HADOOP_YARN_HOME/bin/yarn)" ]]; then
        isYarnExist=true
        yarnCmd="$HADOOP_YARN_HOME/bin/yarn"
    fi

    if [[ $isYarnExist == false ]]; then
        echo "yarn command is not exist, please make sure 'yarn' command is executable or export HADOOP_YARN_HOME"
        exit 1
    fi
}

checkPath() {
    if [[ ! -d $libDir ]]; then
        echo "Please make sure the script is in the compiled directory, which contains lib, conf, log, etc."
        exit 1
    fi
    jarFile=$(ls "$libDir"/$JAR_NAME*.jar)
    if [[ ! -f $jarFile ]]; then
        echo "Please make sure the script is in the compiled directory, and the compiled shuffle-dist jar is in the lib directory."
        exit 1
    fi
}

checkEnv() {
    checkYarnEnv
    checkPath
}

getYarnAppIdByName() {
    appIds=$($yarnCmd application -list | egrep '^application_' | grep "$name" | grep "RUNNING" | awk -v matchName="$name" '{if ($2==matchName) {print $1}}')
    echo $appIds
}

checkAppExist() {
    echo "Checking whether shuffle manager yarn application with name $name is exist"
    oldAppId=$(getYarnAppIdByName)
    if [[ $oldAppId != "" ]]; then
        echo "Shuffle manager with name $name is still running."
        echo "If you want to start multiple shuffle managers, please specify different names for different shuffle managers by -n"
        exit 1
    fi
}

submitYarnJob() {
    if [[ $checkOldJobExist == "True" ]]; then
        checkAppExist
    fi
    runCmd=$(echo "$yarnCmd jar $jarFile $RUN_CLASS $runCmd" | cat)
    echo $runCmd
    $runCmd
}

stopYarnJob() {
    echo "Getting shuffle manager yarn application with name $name"
    appids=$(getYarnAppIdByName)
    if [[ $appids == "" ]]; then
        echo "The shuffle manager with name $name is not running. Stop is not required"
    else
        for appid in $(echo $appids); do
            $yarnCmd application -kill "$appid"
            echo "The shuffle manager application $appid with name $name is stopped"
        done
    fi

}

restartYarnJob() {
    stopYarnJob
    submitYarnJob
}

unrecognizedCmd() {
    echo "Unknown command type. Valid commands include start, stop and restart"
    exit 1
}

main() {
    parseArgs "$@"
    checkEnv
    if [[ $cmdType == "start" ]]; then
        submitYarnJob
    elif [[ $cmdType == "stop" ]]; then
        stopYarnJob
    elif [[ $cmdType == "restart" ]]; then
        restartYarnJob
    else
        unrecognizedCmd
    fi
}

main "$@"
