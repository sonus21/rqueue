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

package com.github.sonus21.rqueue.spring;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueListenerBaseConfig;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.RqueueEndpointManagerImpl;
import com.github.sonus21.rqueue.core.impl.RqueueMessageEnqueuerImpl;
import com.github.sonus21.rqueue.core.impl.RqueueMessageManagerImpl;
import com.github.sonus21.rqueue.core.impl.RqueueMessageSenderImpl;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.metrics.QueueCounter;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.metrics.RqueueMetrics;
import com.github.sonus21.rqueue.metrics.RqueueMetricsCounter;
import com.github.sonus21.rqueue.metrics.RqueueMetricsRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@ComponentScan("com.github.sonus21.rqueue.web")
public class RqueueListenerConfig extends RqueueListenerBaseConfig {

  @Bean
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
  @DependsOn("rqueueConfig")
  public RqueueMessageListenerContainer rqueueMessageListenerContainer(
      RqueueMessageHandler rqueueMessageHandler) {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageHandler() == null) {
      simpleRqueueListenerContainerFactory.setRqueueMessageHandler(rqueueMessageHandler);
    }
    return simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Bean
  public RqueueMessageTemplate rqueueMessageTemplate(RqueueConfig rqueueConfig) {
    return getMessageTemplate(rqueueConfig);
  }

  @Bean
  public RqueueMessageSender rqueueMessageSender(RqueueMessageTemplate rqueueMessageTemplate) {
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageSenderImpl(
          rqueueMessageTemplate, simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageSenderImpl(rqueueMessageTemplate);
  }

  @Bean
  public RqueueMessageManager rqueueMessageManager(RqueueMessageTemplate rqueueMessageTemplate) {
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageManagerImpl(
          rqueueMessageTemplate, simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageManagerImpl(rqueueMessageTemplate);
  }

  @Bean
  public RqueueEndpointManager rqueueEndpointManager(RqueueMessageTemplate rqueueMessageTemplate) {
    return new RqueueEndpointManagerImpl(rqueueMessageTemplate);
  }

  @Bean
  public RqueueMessageEnqueuer rqueueMessageEnqueuer(RqueueMessageTemplate rqueueMessageTemplate) {
    if (simpleRqueueListenerContainerFactory.getMessageConverters() != null) {
      return new RqueueMessageEnqueuerImpl(
          rqueueMessageTemplate, simpleRqueueListenerContainerFactory.getMessageConverters());
    }
    return new RqueueMessageEnqueuerImpl(rqueueMessageTemplate);
  }

  @Bean
  @Conditional(MetricsEnabled.class)
  @DependsOn({"meterRegistry", "rqueueMetricsProperties"})
  public RqueueMetricsRegistry rqueueMetricsRegistry(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> rqueueRedisTemplate) {
    QueueCounter queueCounter = new QueueCounter();
    return new RqueueMetrics(rqueueRedisTemplate, queueCounter);
  }

  @Bean
  @Conditional(MetricsEnabled.class)
  public RqueueMetricsCounter rqueueMetricsCounter(RqueueMetricsRegistry rqueueMetricsRegistry) {
    return new RqueueCounter(rqueueMetricsRegistry.getQueueCounter());
  }
}
