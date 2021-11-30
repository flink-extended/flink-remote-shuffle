################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

HERE=`basename "$PWD"`
if [[ "$HERE" != "flink-remote-shuffle" ]]; then
    echo "Please execute in the flink-remote-shuffle directory";
    exit 1;
fi

mvn clean install -DskipTests

if [[ $? != 0 ]]; then
    echo "Compile error"
    exit 1;
fi

REMOTE_SHUFFLE_VERSION=`xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" ./pom.xml`

REPOSITORY='flink-remote-shuffle'

LOCAL_IMAGE="${REPOSITORY}:${REMOTE_SHUFFLE_VERSION}"

docker rmi ${LOCAL_IMAGE} > /dev/null 2>&1
docker build --build-arg REMOTE_SHUFFLE_VERSION=${REMOTE_SHUFFLE_VERSION} -t ${LOCAL_IMAGE} .
