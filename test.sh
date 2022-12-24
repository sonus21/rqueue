#!/bin/zsh
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
