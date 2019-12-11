/*
 * Copyright 2019 Sonu Kumar
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

package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
public class RqueueMessageAutoConfig extends RqueueConfig {

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageHandler rqueueMessageHandler() {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageHandler() != null) {
      return simpleRqueueListenerContainerFactory.getRqueueMessageHandler();
    }
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageHandler(simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageHandler();
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageListenerContainer rqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler) {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageHandler() == null) {
      simpleRqueueListenerContainerFactory.setRqueueMessageHandler(rqueueMessageHandler);
    }
    if (simpleRqueueListenerContainerFactory.getRedisConnectionFactory() == null) {
      simpleRqueueListenerContainerFactory.setRedisConnectionFactory(getRedisConnectionFactory());
    }
    return simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageSender rqueueMessageSender() {
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageSender(
          getMessageTemplate(getRedisConnectionFactory()),
          simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageSender(getMessageTemplate(getRedisConnectionFactory()));
  }

  @Bean
  @ConditionalOnMissingBean
  public RedisMessageListenerContainer redisMessageListenerContainer() {
    RedisMessageListenerContainer messageListenerContainer = new RedisMessageListenerContainer();
    messageListenerContainer.setConnectionFactory(getRedisConnectionFactory());
    return messageListenerContainer;
  }
}
