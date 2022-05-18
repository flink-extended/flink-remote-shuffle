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

# Start a ShuffleManager on Kubernetes.

USAGE="Usage: kubernetes-shufflemanager.sh [args]"

ARGS=("${@:1}")

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/config.sh

export LOG_PREFIX=${SHUFFLE_LOG_DIR}"/shufflemanager"

parseShuffleManagerJvmArgsAndExportLogs "${ARGS[@]}"

exec "${SHUFFLE_BIN_DIR}"/shuffle-console.sh "kubernetes-shufflemanager" "${ARGS[@]}"
