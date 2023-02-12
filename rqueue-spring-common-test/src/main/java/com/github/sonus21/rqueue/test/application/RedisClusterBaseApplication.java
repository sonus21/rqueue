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

package com.github.sonus21.rqueue.test.application;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.utils.Constants;
import io.lettuce.core.ReadFrom;
import java.util.ArrayList;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

public abstract class RedisClusterBaseApplication extends ApplicationBasicConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    LettuceClientConfiguration lettuceClientConfiguration =
        LettuceClientConfiguration.builder().readFrom(ReadFrom.MASTER_PREFERRED).build();

    RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
    List<RedisNode> redisNodes = new ArrayList<>();
    redisNodes.add(new RedisNode("127.0.0.1", 9000));
    redisNodes.add(new RedisNode("127.0.0.1", 9001));
    redisNodes.add(new RedisNode("127.0.0.1", 9002));
    redisNodes.add(new RedisNode("127.0.0.1", 9003));
    redisNodes.add(new RedisNode("127.0.0.1", 9004));
    redisNodes.add(new RedisNode("127.0.0.1", 9005));
    redisClusterConfiguration.setClusterNodes(redisNodes);

    LettuceConnectionFactory lettuceConnectionFactory =
        new LettuceConnectionFactory(redisClusterConfiguration, lettuceClientConfiguration);
    lettuceConnectionFactory.afterPropertiesSet();
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(lettuceConnectionFactory);
    simpleRqueueListenerContainerFactory.setPollingInterval(Constants.ONE_MILLI);
    if (reactiveEnabled) {
      simpleRqueueListenerContainerFactory.setReactiveRedisConnectionFactory(
          lettuceConnectionFactory);
    }
    return simpleRqueueListenerContainerFactory;
  }
}
