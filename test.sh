#!/bin/zsh
#
# Copyright (c) 2021-2023 Sonu Kumar
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.
#
#

function runTests(){
  ./gradlew codeCoverageReport -DincludeTags=unit
  ./gradlew codeCoverageReport -DincludeTags=producerOnly
  ./gradlew codeCoverageReport -DincludeTags=integration -DexcludeTags=redisCluster,producerOnly,local
  ./gradlew codeCoverageReport -DincludeTags=redisCluster
  ./gradlew codeCoverageReport -DincludeTags=local

}
echo "Starting to run tests  $(date)"
runTests
echo "Ended run tests  $(date)"

export RQUEUE_REACTIVE_ENABLED=true
echo "Starting to run reactive redis tests  $(date)"
runTests
echo "ended reactive redis tests  $(date)"
