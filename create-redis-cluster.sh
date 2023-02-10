#!/usr/bin/env sh

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

if [ -z "$1" ]; then
    echo "You must pass directory name"
    printf "Usage\n"
    echo "create-redis-cluster.sh <directory> --fresh "
    exit
fi
rm -r "$1"
mkdir "$1"
cd "$1" && mkdir 9000 9001 9002 9003 9004 9005
printf "port 9000 \ncluster-enabled yes \ncluster-config-file nodes.conf \ncluster-node-timeout 5000 \nappendonly yes" >> "9000/redis.conf"
printf "port 9001 \ncluster-enabled yes \ncluster-config-file nodes.conf \ncluster-node-timeout 5000 \nappendonly yes" >> "9001/redis.conf"
printf "port 9002 \ncluster-enabled yes \ncluster-config-file nodes.conf \ncluster-node-timeout 5000 \nappendonly yes" >> "9002/redis.conf"
printf "port 9003 \ncluster-enabled yes \ncluster-config-file nodes.conf \ncluster-node-timeout 5000 \nappendonly yes" >> "9003/redis.conf"
printf "port 9004 \ncluster-enabled yes \ncluster-config-file nodes.conf \ncluster-node-timeout 5000 \nappendonly yes" >> "9004/redis.conf"
printf "port 9005 \ncluster-enabled yes \ncluster-config-file nodes.conf \ncluster-node-timeout 5000 \nappendonly yes" >> "9005/redis.conf"
cd "9000" && redis-server ./redis.conf &
cd "9001" && redis-server ./redis.conf &
cd "9002" && redis-server ./redis.conf &
cd "9003" && redis-server ./redis.conf &
cd "9004" && redis-server ./redis.conf &
cd "9005" && redis-server ./redis.conf &
sleep 5
redis-cli --cluster create 127.0.0.1:9000 127.0.0.1:9001 127.0.0.1:9002 127.0.0.1:9003 127.0.0.1:9004 127.0.0.1:9005 --cluster-replicas 1 --cluster-yes




