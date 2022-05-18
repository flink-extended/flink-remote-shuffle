#!/usr/bin/env bash
################################################################################
# Copyright 2021 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# Start a shuffle component as a console application. Must be stopped with Ctrl-C
# or with SIGTERM by kill or the controlling process.
USAGE="Usage: shuffle-console.sh (kubernetes-shufflemanager|kubernetes-shuffleworker|shufflemanager|shuffleworker) [args]"

SERVICE=$1
ARGS=("${@:2}") # get remaining arguments as array

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

case $SERVICE in
    (kubernetes-shufflemanager)
        CLASS_TO_RUN=com.alibaba.flink.shuffle.kubernetes.manager.KubernetesShuffleManagerRunner
    ;;

    (kubernetes-shuffleworker)
        CLASS_TO_RUN=com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner
    ;;

    (shufflemanager)
        CLASS_TO_RUN=com.alibaba.flink.shuffle.coordinator.manager.ShuffleManagerRunner
    ;;

    (shuffleworker)
        CLASS_TO_RUN=com.alibaba.flink.shuffle.coordinator.worker.ShuffleWorkerRunner
    ;;

    (*)
        echo "Unknown service '${SERVICE}'. $USAGE."
        exit 1
    ;;
esac

SHUFFLE_CLASSPATH=`constructShuffleClassPath`

SHUFFLE_PID_DIR="/tmp"
pid=$SHUFFLE_PID_DIR/$SERVICE.pid
# The lock needs to be released after use because this script is started foreground
command -v flock >/dev/null 2>&1
flock_exist=$?
if [[ ${flock_exist} -eq 0 ]]; then
    exec 200<"$SHUFFLE_PID_DIR"
    flock 200
fi
# Remove the pid file when all the processes are dead
if [ -f "$pid" ]; then
    all_dead=0
    while read each_pid; do
        # Check whether the process is still running
        kill -0 $each_pid > /dev/null 2>&1
        [[ $? -eq 0 ]] && all_dead=1
    done < "$pid"
    [ ${all_dead} -eq 0 ] && rm $pid
fi
id=$([ -f "$pid" ] && echo $(wc -l < "$pid") || echo "0")

if [ -z "${LOG_PREFIX}" ]; then
    LOG_PREFIX="${SHUFFLE_LOG_DIR}/${SERVICE}-${id}-${HOSTNAME}"
fi

log="${LOG_PREFIX}.log"

log_setting=("-Dlog.file=${log}" "-Dlog4j.configurationFile=file:${SHUFFLE_CONF_DIR}/log4j2.properties")

echo "Starting $SERVICE as a console application on host $HOSTNAME."

# Add the current process id to pid file
echo $$ >> "$pid" 2>/dev/null

# Release the lock because the java process runs in the foreground and would block other processes from modifying the pid file
[[ ${flock_exist} -eq 0 ]] &&  flock -u 200

exec "$JAVA_RUN" "${log_setting[@]}" $JVM_ARGS -classpath "`manglePathList "$SHUFFLE_CLASSPATH"`" ${CLASS_TO_RUN} "${ARGS[@]}" -c ${SHUFFLE_CONF_DIR}
