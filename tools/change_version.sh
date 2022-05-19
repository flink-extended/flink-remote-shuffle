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

OLD_VERSION="1.0-SNAPSHOT"
NEW_VERSION="1.1-SNAPSHOT"

bin=`dirname "$0"`
cd $bin/../

# change version in all pom files
find .. -name 'pom.xml' -type f -exec perl -pi -e 's#<version>'"$OLD_VERSION"'</version>#<version>'"$NEW_VERSION"'</version>#' {} \;
