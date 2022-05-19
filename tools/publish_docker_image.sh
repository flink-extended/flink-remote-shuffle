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
cd $bin/../

REGISTRY='docker.io'
NAMESPACE='flinkremoteshuffle'
REPOSITORY='flink-remote-shuffle'

if [[ "$1" ]] ; then
    REGISTRY="$1"
fi

if [[ "$2" ]] ; then
    NAMESPACE="$2"
fi

if [[ "$3" ]] ; then
    REPOSITORY="$3"
fi

REMOTE_SHUFFLE_VERSION=`xmllint --xpath "//*[local-name()='project']/*[local-name()='version']/text()" ./pom.xml`
LOCAL_IMAGE="${REPOSITORY}:${REMOTE_SHUFFLE_VERSION}"
REMOTE_IMAGE="${REGISTRY}/${NAMESPACE}/${REPOSITORY}:${REMOTE_SHUFFLE_VERSION}"
echo $REMOTE_IMAGE

sh ./tools/build_docker_image.sh

if [[ $? != 0 ]]; then
    echo "Build docker image error"
    exit 1;
fi

docker rmi ${REMOTE_IMAGE} > /dev/null 2>&1
docker tag ${LOCAL_IMAGE} ${REMOTE_IMAGE}
docker push ${REMOTE_IMAGE}
