
./gradlew codeCoverageReport -DincludeTags=unit
./gradlew codeCoverageReport -DincludeTags=producerOnly
./gradlew codeCoverageReport -DincludeTags=integration -DexcludeTags=redisCluster,producerOnly,local
./gradlew codeCoverageReport -DincludeTags=redisCluster
./gradlew codeCoverageReport -DincludeTags=local

export RQUEUE_REACTIVE_ENABLED=true

./gradlew codeCoverageReport -DincludeTags=unit
./gradlew codeCoverageReport -DincludeTags=producerOnly
./gradlew codeCoverageReport -DincludeTags=integration -DexcludeTags=redisCluster,producerOnly,local
./gradlew codeCoverageReport -DincludeTags=redisCluster
./gradlew codeCoverageReport -DincludeTags=local
