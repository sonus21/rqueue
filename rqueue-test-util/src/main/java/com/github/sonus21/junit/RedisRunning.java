/*
 * Copyright (c) 2020-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.junit;

import java.util.List;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

public class RedisRunning {

  private final List<RedisNode> redisNodes;
  private RedisConnectionFactory redisConnectionFactory;

  RedisRunning(List<RedisNode> redisNodes) {
    this.redisNodes = redisNodes;
  }

  public static boolean fatal() {
    return System.getenv("REDIS_RUNNING") != null;
  }

  private void initConnectionFactory() {
    LettuceConnectionFactory lettuceConnectionFactory;
    if (this.redisNodes.size() == 1) {
      RedisNode redisNode = this.redisNodes.get(0);
      lettuceConnectionFactory =
          new LettuceConnectionFactory(redisNode.getHost(), redisNode.getPort());
    } else {
      LettuceClientConfiguration lettuceClientConfiguration =
          LettuceClientConfiguration.builder().build();
      RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
      redisClusterConfiguration.setClusterNodes(redisNodes);
      lettuceConnectionFactory =
          new LettuceConnectionFactory(redisClusterConfiguration, lettuceClientConfiguration);
    }
    lettuceConnectionFactory.afterPropertiesSet();
    this.redisConnectionFactory = lettuceConnectionFactory;
  }

  public synchronized void isUp() throws Exception {
    if (this.redisConnectionFactory == null) {
      initConnectionFactory();
    }
    RedisConnection redisConnection = null;
    try {
      redisConnection = redisConnectionFactory.getConnection();
      String result = redisConnection.ping();
      if (result != null && result.equalsIgnoreCase("pong")) {
        return;
      }
      throw new IllegalStateException("Redis ping is not working");
    } finally {
      if (redisConnection != null) {
        redisConnection.close();
      }
    }
  }
}
