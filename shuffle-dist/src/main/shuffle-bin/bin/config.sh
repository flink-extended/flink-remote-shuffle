#!/usr/bin/env bash
################################################################################
# Copyright 2021 The Flink Remote Shuffle Project
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

bin=`dirname "$0"`
SHUFFLE_BIN_DIR=`cd "$bin"; pwd`

# Define shuffle dir.
SHUFFLE_HOME=`dirname "$SHUFFLE_BIN_DIR"`
if [ -z "$SHUFFLE_LIB_DIR" ]; then SHUFFLE_LIB_DIR=$SHUFFLE_HOME/lib; fi
if [ -z "$SHUFFLE_CONF_DIR" ]; then SHUFFLE_CONF_DIR=$SHUFFLE_HOME/conf; fi
if [ -z "$SHUFFLE_LOG_DIR" ]; then SHUFFLE_LOG_DIR=$SHUFFLE_HOME/log; fi

# Define HOSTNAME if it is not already set
if [ -z "${HOSTNAME}" ]; then
    HOSTNAME=`hostname`
fi

UNAME=$(uname -s)
if [ "${UNAME:0:6}" == "CYGWIN" ]; then
    JAVA_RUN=java
else
    if [[ -d "$JAVA_HOME" ]]; then
        JAVA_RUN="$JAVA_HOME"/bin/java
    else
        JAVA_RUN=java
    fi
fi

SHUFFLE_CONF_FILE="remote-shuffle-conf.yaml"
YAML_CONF=${SHUFFLE_CONF_DIR}/${SHUFFLE_CONF_FILE}

manglePathList() {
    UNAME=$(uname -s)
    # a path list, for example a java classpath
    if [ "${UNAME:0:6}" == "CYGWIN" ]; then
        echo `cygpath -wp "$1"`
    else
        echo $1
    fi
}

# Looks up a config value by key from a simple YAML-style key-value map.
# $1: key to look up
# $2: default value to return if key does not exist
# $3: config file to read from
readFromConfig() {
    local key=$1
    local defaultValue=$2
    local configFile=$3

    # first extract the value with the given key (1st sed), then trim the result (2nd sed)
    # if a key exists multiple times, take the "last" one (tail)
    local value=`sed -n "s/^[ ]*${key}[ ]*: \([^#]*\).*$/\1/p" "${configFile}" | sed "s/^ *//;s/ *$//" | tail -n 1`

    [ -z "$value" ] && echo "$defaultValue" || echo "$value"
}

constructShuffleClassPath() {
    local SHUFFLE_DIST
    local SHUFFLE_CLASSPATH

    while read -d '' -r jarfile ; do
        if [[ "$jarfile" =~ .*/shuffle-dist[^/]*.jar$ ]]; then
            SHUFFLE_DIST="$SHUFFLE_DIST":"$jarfile"
        elif [[ "$SHUFFLE_CLASSPATH" == "" ]]; then
            SHUFFLE_CLASSPATH="$jarfile";
        else
            SHUFFLE_CLASSPATH="$SHUFFLE_CLASSPATH":"$jarfile"
        fi
    done < <(find "$SHUFFLE_LIB_DIR" ! -type d -name '*.jar' -print0 | sort -z)

    if [[ "$SHUFFLE_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Shuffle distribution jar not found in $SHUFFLE_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$SHUFFLE_CLASSPATH""$SHUFFLE_DIST"
}

findShuffleDistJar() {
    local SHUFFLE_DIST="`find "$SHUFFLE_LIB_DIR" -name 'shuffle-dist*.jar'`"

    if [[ "$SHUFFLE_DIST" == "" ]]; then
        # write error message to stderr since stdout is stored as the classpath
        (>&2 echo "[ERROR] Shuffle distribution jar not found in $SHUFFLE_LIB_DIR.")

        # exit function with empty classpath to force process failure
        exit 1
    fi

    echo "$SHUFFLE_DIST"
}

########################################################################################################################
# DEFAULT CONFIG VALUES: These values will be used when nothing has been specified in conf/remote-shuffle-conf.yaml
# -or- the respective environment variables are not set.
########################################################################################################################

DEFAULT_ENV_SSH_OPTS=""                             # Optional SSH parameters running in cluster mode

########################################################################################################################
# CONFIG KEYS: The default values can be overwritten by the following keys in conf/remote-shuffle-conf.yaml
########################################################################################################################

KEY_ENV_SSH_OPTS="env.ssh.opts"

if [ -z "${SHUFFLE_SSH_OPTS}" ]; then
    SHUFFLE_SSH_OPTS=$(readFromConfig ${KEY_ENV_SSH_OPTS} "${DEFAULT_ENV_SSH_OPTS}" "${YAML_CONF}")
fi

extractLoggingOutputs() {
    local output="$1"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"

    echo "${output}" | grep -v ${EXECUTION_PREFIX}
}

extractExecutionResults() {
    local output="$1"
    local expected_lines="$2"
    local EXECUTION_PREFIX="BASH_JAVA_UTILS_EXEC_RESULT:"
    local execution_results
    local num_lines

    execution_results=$(echo "${output}" | grep ${EXECUTION_PREFIX})
    num_lines=$(echo "${execution_results}" | wc -l)
    # explicit check for empty result, becuase if execution_results is empty, then wc returns 1
    if [[ -z ${execution_results} ]]; then
        echo "[ERROR] The execution result is empty." 1>&2
        exit 1
    fi
    if [[ ${num_lines} -ne ${expected_lines} ]]; then
        echo "[ERROR] The execution results has unexpected number of lines, expected: ${expected_lines}, actual: ${num_lines}." 1>&2
        echo "[ERROR] An execution result line is expected following the prefix '${EXECUTION_PREFIX}'" 1>&2
        echo "$output" 1>&2
        exit 1
    fi

    echo "${execution_results//${EXECUTION_PREFIX}/}"
}

runBashJavaUtilsCmd() {
    local cmd=$1
    local class_path="$(constructShuffleClassPath)"
    local dynamic_args=("${@:2}")
    local java_utils_log_conf="/tmp/log4j2-bash-java-utils.properties"

    # log config for bash java utils.
    cat > ${java_utils_log_conf} << EOF
    rootLogger.level = INFO
    rootLogger.appenderRef.console.ref = ConsoleAppender
    appender.console.name = ConsoleAppender
    appender.console.type = CONSOLE
    appender.console.layout.type = PatternLayout
    appender.console.layout.pattern = %-5p %x - %m%n
EOF

    local log_setting=("-Dlog4j.configurationFile=file:${java_utils_log_conf}")
    local output=`"${JAVA_RUN}" "${log_setting}" -classpath "${class_path}" com.alibaba.flink.shuffle.core.utils.BashJavaUtils ${cmd} "${dynamic_args[@]}" -c ${SHUFFLE_CONF_DIR} 2>&1 | tail -n 1000`
    if [[ $? -ne 0 ]]; then
        echo "[ERROR] Cannot run BashJavaUtils to execute command ${cmd}." 1>&2
        # Print the output in case the user redirect the log to console.
        echo "$output" 1>&2
        exit 1
    fi

    echo "$output"
}

parseJvmArgsAndExportLogs() {
  args=("${@}")
  java_utils_output=$(runBashJavaUtilsCmd "${args[@]}")
  logging_output=$(extractLoggingOutputs "${java_utils_output}")
  params_output=$(extractExecutionResults "${java_utils_output}" 1)

  if [[ $? -ne 0 ]]; then
    echo "[ERROR] Could not get JVM parameters properly."
    echo "[ERROR] Raw output from BashJavaUtils:"
    echo "$java_utils_output"
    exit 1
  fi

  export JVM_ARGS="$(echo "${params_output}" | head -n1)"
  export SHUFFLE_INHERITED_LOGS="
JVM params and resource extraction logs:
jvm_opts: $JVM_ARGS
logs: $logging_output
"
}

parseShuffleManagerJvmArgsAndExportLogs() {
  args=("${@}")
  parseJvmArgsAndExportLogs GET_SHUFFLE_MANAGER_JVM_PARAMS "${args[@]}"
}

parseShuffleWorkerJvmArgsAndExportLogs() {
  args=("${@}")
  parseJvmArgsAndExportLogs GET_SHUFFLE_WORKER_JVM_PARAMS "${args[@]}"
}

extractHostName() {
    # handle comments: extract first part of string (before first # character)
    WORKER=`echo $1 | cut -d'#' -f 1`

    # Extract the hostname from the network hierarchy
    if [[ "$WORKER" =~ ^.*/([0-9a-zA-Z.-]+)$ ]]; then
            WORKER=${BASH_REMATCH[1]}
    fi

    echo $WORKER
}

readManagers() {
    MANAGERS_FILE="${SHUFFLE_CONF_DIR}/managers"

    if [[ ! -f "${MANAGERS_FILE}" ]]; then
        echo "No managers file. Please specify managers in 'conf/managers'."
        exit 1
    fi

    MANAGERS=()

    MANAGERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ] ; then
            if [ "${HOST}" == "localhost" ] ; then
                HOST="127.0.0.1"
            fi
            MANAGERS+=(${HOST})
            if [ "${HOST}" != "127.0.0.1" ] ; then
                MANAGERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$MANAGERS_FILE"

    # when supports multiple shuffle managers, only need to remove the code below.
    if [[ "${#MANAGERS[@]}" -gt 1 ]]; then
        echo "at present, only one shuffle manager can be started, using the first ip address in managers file."
        MANAGERS=(${MANAGERS[0]})
    fi
}

readWorkers() {
    WORKERS_FILE="${SHUFFLE_CONF_DIR}/workers"

    if [[ ! -f "$WORKERS_FILE" ]]; then
        echo "No workers file. Please specify workers in 'conf/workers'."
        exit 1
    fi

    WORKERS=()

    WORKERS_ALL_LOCALHOST=true
    GOON=true
    while $GOON; do
        read line || GOON=false
        HOST=$( extractHostName $line)
        if [ -n "$HOST" ] ; then
            WORKERS+=(${HOST})
            if [ "${HOST}" != "localhost" ] && [ "${HOST}" != "127.0.0.1" ] ; then
                WORKERS_ALL_LOCALHOST=false
            fi
        fi
    done < "$WORKERS_FILE"
}

# starts or stops shuffleManagers on specified IP address.
# note that this is for HA in the future. At present, only one shuffle manager can be started.
# usage: ShuffleManagers start|stop
ShuffleManagers() {
    CMD=$1

    readManagers
    MANAGERS_ARGS=("${@:2}")
    MANAGERS_ARGS+=(-D remote-shuffle.manager.rpc-address=${MANAGERS[@]})

    if [ ${MANAGERS_ALL_LOCALHOST} = true ] ; then
        # all-local setup
        for manager in ${MANAGERS[@]}; do
            "${SHUFFLE_BIN_DIR}"/shufflemanager.sh "${CMD}" "${MANAGERS_ARGS[@]}"
        done
    else
        # non-local setup
        # start/stop shuffleManager instance(s) using pdsh (Parallel Distributed Shell) when available
        TMP_ARGS=()
        for marg in ${MANAGERS_ARGS[@]}; do
          if [ $marg = "-D" ] ; then
            TMP_ARGS+=(-D)
          else
            TMP_ARGS+=(\"$marg\")
          fi
        done
        SSH_MANAGER_ARGS=${TMP_ARGS[@]}

        command -v pdsh >/dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            for manager in ${MANAGERS[@]}; do
                ssh -n $SHUFFLE_SSH_OPTS $manager -- "nohup /bin/bash -l \"${SHUFFLE_BIN_DIR}/shufflemanager.sh\" \"${CMD}\" $SSH_MANAGER_ARGS &"
            done
        else
            PDSH_SSH_ARGS="" PDSH_SSH_ARGS_APPEND=$SHUFFLE_SSH_OPTS pdsh -w $(IFS=, ; echo "${MANAGERS[*]}") \
                "nohup /bin/bash -l \"${SHUFFLE_BIN_DIR}/shufflemanager.sh\" \"${CMD}\" $SSH_MANAGER_ARGS "
        fi
    fi
}

# starts or stops shuffleWorkers on all workers
# usage: ShuffleWorkers start|stop
ShuffleWorkers() {
    CMD=$1

    WORKER_ARGS=("${@:2}")
    WORKER_ARGS+=(-D remote-shuffle.manager.rpc-address="${MANAGERS[@]}")

    readWorkers

    if [ ${WORKERS_ALL_LOCALHOST} = true ] ; then
        # all-local setup
        for worker in ${WORKERS[@]}; do
            "${SHUFFLE_BIN_DIR}"/shuffleworker.sh "${CMD}" "${WORKER_ARGS[@]}"
        done
    else
        # non-local setup
        # start/stop shuffleWorker instance(s) using pdsh (Parallel Distributed Shell) when available
        TMP_ARGS=()
        for warg in ${WORKER_ARGS[@]}; do
          if [ $warg = "-D" ] ; then
            TMP_ARGS+=(-D)
          else
            TMP_ARGS+=(\"$warg\")
          fi
        done
        SSH_WORKER_ARGS=${TMP_ARGS[@]}
        command -v pdsh >/dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            for worker in ${WORKERS[@]}; do
                ssh -n $SHUFFLE_SSH_OPTS $worker -- "nohup /bin/bash -l \"${SHUFFLE_BIN_DIR}/shuffleworker.sh\" \"${CMD}\" $SSH_WORKER_ARGS &"
            done
        else
            PDSH_SSH_ARGS="" PDSH_SSH_ARGS_APPEND=$SHUFFLE_SSH_OPTS pdsh -w $(IFS=, ; echo "${WORKERS[*]}") \
                "nohup /bin/bash -l \"${SHUFFLE_BIN_DIR}/shuffleworker.sh\" \"${CMD}\" $SSH_WORKER_ARGS "
        fi
    fi
}

