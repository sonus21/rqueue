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

package com.github.sonus21.rqueue.config;

import static com.github.sonus21.rqueue.utils.RedisUtils.getRedisTemplate;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.common.impl.RqueueLockManagerImpl;
import com.github.sonus21.rqueue.core.DelayedMessageScheduler;
import com.github.sonus21.rqueue.core.ProcessingMessageScheduler;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.core.RqueueRedisListenerContainerFactory;
import com.github.sonus21.rqueue.web.view.DateTimeFunction;
import org.jtwig.environment.EnvironmentConfiguration;
import org.jtwig.environment.EnvironmentConfigurationBuilder;
import org.jtwig.spring.JtwigViewResolver;
import org.jtwig.web.servlet.JtwigRenderer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * This is a base configuration class for Rqueue, that is used in Spring and Spring boot Rqueue libs
 * for configurations. This class creates required beans to work Rqueue library.
 *
 * <p>It internally maintains two types of scheduled tasks for different functionality, for delayed
 * queue messages have to be moved from ZSET to LIST, in other case to at least once message
 * delivery guarantee, messages have to be moved from ZSET to LIST again, we expect very small
 * number of messages in processing queue. Reason being we delete messages once it's consumed, but
 * due to failure in listeners message might not be removed, whereas message in a delayed queue can
 * be very high based on the use case.
 */
public abstract class RqueueListenerBaseConfig {

  @Autowired(required = false)
  protected final SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
      new SimpleRqueueListenerContainerFactory();

  /**
   * Create Rqueue configuration bean either from listener container factory or from bean factory.
   * 1st priority is given to container factory. This redis connection factory is used to connect to
   * Database for different ops.
   *
   * @param beanFactory configurable bean factory
   * @return {@link RedisConnectionFactory} object.
   */
  @Bean
  public RqueueConfig rqueueConfig(ConfigurableBeanFactory beanFactory) {
    boolean sharedConnection = false;
    if (simpleRqueueListenerContainerFactory.getRedisConnectionFactory() == null) {
      sharedConnection = true;
      simpleRqueueListenerContainerFactory.setRedisConnectionFactory(
          beanFactory.getBean(RedisConnectionFactory.class));
    }
    return new RqueueConfig(
        simpleRqueueListenerContainerFactory.getRedisConnectionFactory(), sharedConnection);
  }

  @Bean
  public RqueueWebConfig rqueueWebConfig() {
    return new RqueueWebConfig();
  }

  @Bean
  public RqueueSchedulerConfig rqueueSchedulerConfig() {
    return new RqueueSchedulerConfig();
  }

  /**
   * Get Rqueue message template either from listener container factory or create new one. 1st
   * priority is given to container factory. Message template is used to serialize message and
   * sending message to Redis.
   *
   * @param rqueueConfig rqueue config object
   * @return {@link RqueueMessageTemplate} object
   */
  protected RqueueMessageTemplate getMessageTemplate(RqueueConfig rqueueConfig) {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageTemplate() != null) {
      return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
    }
    simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(
        new RqueueMessageTemplateImpl(rqueueConfig.getConnectionFactory()));
    return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
  }

  @Bean
  public RedisTemplate<String, Long> rqueueRedisLongTemplate(RqueueConfig rqueueConfig) {
    return getRedisTemplate(rqueueConfig.getConnectionFactory());
  }

  @Bean
  public RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory() {
    return new RqueueRedisListenerContainerFactory();
  }

  /**
   * This scheduler is used to pull messages from a delayed queue to their respective queue.
   * Internally it moves messages from ZSET to LIST based on the priority and current time.
   *
   * @return {@link DelayedMessageScheduler} object
   */
  @Bean
  public DelayedMessageScheduler delayedMessageScheduler() {
    return new DelayedMessageScheduler();
  }

  /**
   * This scheduler is used to pull messages from processing queue to their respective queue.
   * Internally it moves messages from ZSET to LIST based on the priority and current time.
   *
   * @return {@link ProcessingMessageScheduler} object
   */
  @Bean
  public ProcessingMessageScheduler processingMessageScheduler() {
    return new ProcessingMessageScheduler();
  }

  @Bean
  public RqueueRedisTemplate<String> stringRqueueRedisTemplate(RqueueConfig rqueueConfig) {
    return new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Bean
  public RqueueLockManager rqueueLockManager(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> rqueueRedisTemplate) {
    return new RqueueLockManagerImpl(rqueueRedisTemplate);
  }

  @Bean
  public JtwigViewResolver rqueueViewResolver() {
    EnvironmentConfiguration configuration =
        EnvironmentConfigurationBuilder.configuration()
            .functions()
            .add(new DateTimeFunction())
            .and()
            .build();
    JtwigRenderer renderer = new JtwigRenderer(configuration);
    JtwigViewResolver viewResolver = new JtwigViewResolver();
    viewResolver.setRenderer(renderer);
    viewResolver.setPrefix("classpath:/templates/rqueue/");
    viewResolver.setSuffix(".html");
    return viewResolver;
  }
}
