/*
 * Copyright (c) 2019-2023 Sonu Kumar
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
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import redis.embedded.RedisServer;

@Slf4j
public abstract class MultiRedisSprigBaseApplication extends ApplicationBasicConfiguration {

  @Value("${spring.data.redis2.port}")
  private int redisPort2;

  @Value("${spring.data.redis2.host}")
  private String redisHost2;

  private RedisServer redisServer2;

  @PostConstruct
  public void postConstruct() {
    init();
    if (redisServer2 == null) {
      redisServer2 = new RedisServer(redisPort2);
      redisServer2.start();
    }
    monitor(redisHost, redisPort);
  }

  @PreDestroy
  public void preDestroy() {
    cleanup();
    if (redisServer2 != null) {
      redisServer2.stop();
    }
    for (MonitorProcess monitorProcess : processes) {
      for (String line : monitorProcess.out) {
        assert line.equals("OK");
      }
    }
  }

  @Bean
  public LettuceConnectionFactory redisConnectionFactory() {
    return new LettuceConnectionFactory(redisHost, redisPort);
  }

  @Bean
  public RedisMessageListenerContainer container(RedisConnectionFactory redisConnectionFactory) {
    RedisMessageListenerContainer container = new RedisMessageListenerContainer();
    container.setConnectionFactory(redisConnectionFactory);
    return container;
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    RedisStandaloneConfiguration redisConfiguration =
        new RedisStandaloneConfiguration(redisHost2, redisPort2);
    LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration);
    connectionFactory.afterPropertiesSet();
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(connectionFactory);
    if (reactiveEnabled) {
      simpleRqueueListenerContainerFactory.setReactiveRedisConnectionFactory(connectionFactory);
    }
    return simpleRqueueListenerContainerFactory;
  }
}
