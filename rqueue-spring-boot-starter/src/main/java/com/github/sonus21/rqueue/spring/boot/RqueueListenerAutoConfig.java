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

package com.github.sonus21.rqueue.spring.boot;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueListenerBaseConfig;
import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.ReactiveRqueueMessageEnqueuerImpl;
import com.github.sonus21.rqueue.core.impl.RqueueEndpointManagerImpl;
import com.github.sonus21.rqueue.core.impl.RqueueMessageEnqueuerImpl;
import com.github.sonus21.rqueue.core.impl.RqueueMessageManagerImpl;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.utils.condition.ReactiveEnabled;
import com.github.sonus21.rqueue.utils.condition.RqueueEnabled;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@AutoConfigureAfter(RedisAutoConfiguration.class)
@ComponentScan({"com.github.sonus21.rqueue.web", "com.github.sonus21.rqueue.dao"})
@Conditional({RqueueEnabled.class})
public class RqueueListenerAutoConfig extends RqueueListenerBaseConfig {

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageHandler rqueueMessageHandler() {
    return simpleRqueueListenerContainerFactory.getRqueueMessageHandler(
        getMessageConverterProvider());
  }

  @Bean
  @DependsOn("rqueueConfig")
  @ConditionalOnMissingBean
  public RqueueMessageListenerContainer rqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler) {
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(rqueueMessageHandler);
    return simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageTemplate rqueueMessageTemplate(
      RqueueConfig rqueueConfig, RqueueMessageHandler rqueueMessageHandler) {
    return getMessageTemplate(rqueueConfig);
  }


  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageManager rqueueMessageManager(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    return new RqueueMessageManagerImpl(
        rqueueMessageTemplate,
        rqueueMessageHandler.getMessageConverter(),
        simpleRqueueListenerContainerFactory.getMessageHeaders());
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueEndpointManager rqueueEndpointManager(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    return new RqueueEndpointManagerImpl(
        rqueueMessageTemplate,
        rqueueMessageHandler.getMessageConverter(),
        simpleRqueueListenerContainerFactory.getMessageHeaders());
  }

  @Bean
  @ConditionalOnMissingBean
  public RqueueMessageEnqueuer rqueueMessageEnqueuer(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    return new RqueueMessageEnqueuerImpl(
        rqueueMessageTemplate,
        rqueueMessageHandler.getMessageConverter(),
        simpleRqueueListenerContainerFactory.getMessageHeaders());
  }

  @Bean
  @ConditionalOnMissingBean
  @Conditional(ReactiveEnabled.class)
  public ReactiveRqueueMessageEnqueuer reactiveRqueueMessageEnqueuer(
      RqueueMessageHandler rqueueMessageHandler, RqueueMessageTemplate rqueueMessageTemplate) {
    return new ReactiveRqueueMessageEnqueuerImpl(
        rqueueMessageTemplate,
        rqueueMessageHandler.getMessageConverter(),
        simpleRqueueListenerContainerFactory.getMessageHeaders());
  }
}
