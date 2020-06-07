/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.broker.config;

import com.github.sonus21.rqueue.broker.service.impl.RedisMessageListenerImpl;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueRedisListenerContainerFactory;
import java.util.Collections;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.listener.ChannelTopic;

@Configuration
public class RqueueBrokerConfig {
  private final RqueueConfig rqueueConfig;

  @Autowired
  public RqueueBrokerConfig(RqueueConfig rqueueConfig) {
    this.rqueueConfig = rqueueConfig;
  }

  @Autowired
  public void subscribeTopic(
      RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory,
      ChannelTopic channelTopic) {
    rqueueRedisListenerContainerFactory
        .getContainer()
        .addMessageListener(redisMessageListener(), Collections.singleton(channelTopic));
  }

  @Bean
  ChannelTopic channelTopic() {
    return new ChannelTopic(rqueueConfig.getInternalEventChannel());
  }

  @Bean
  public RedisMessageListenerImpl redisMessageListener() {
    return new RedisMessageListenerImpl();
  }
}
