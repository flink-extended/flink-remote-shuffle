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

FROM openjdk:8-jdk-slim
ARG REMOTE_SHUFFLE_VERSION

# Install some utilities
RUN apt-get update \
     && apt-get install -y netcat dnsutils less procps iputils-ping \
                 libssl-dev \
                 curl \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

RUN set -e && mkdir -p /flink-remote-shuffle

RUN ln -s /flink-remote-shuffle /flink-shuffle

COPY ./shuffle-dist/target/flink-remote-shuffle-${REMOTE_SHUFFLE_VERSION}-bin/flink-remote-shuffle-${REMOTE_SHUFFLE_VERSION} /flink-remote-shuffle

WORKDIR /flink-remote-shuffle

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV LC_ALL en_US.UTF-8
