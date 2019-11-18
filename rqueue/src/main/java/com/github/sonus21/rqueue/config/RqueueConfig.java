/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.config;

import static com.github.sonus21.rqueue.utils.RedisUtil.getRedisTemplate;

import com.github.sonus21.rqueue.core.MessageScheduler;
import com.github.sonus21.rqueue.core.ProcessingMessageScheduler;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * This is a base configuration class, that is used in Spring and Spring boot Rqueue packages for
 * configuration. This class creates required beans to work Rqueue library. It internally maintains
 * two types of scheduled tasks for different functionality, for delayed queue messages have to be
 * moved from ZSET to LIST, in other case to at least once message delivery guarantee, messages have
 * to be moved from ZSET to LIST again, we expect very small number of messages in processing queue.
 * Reason being we delete messages once it's consumed, but due to failure in listeners message might
 * not be removed, where as message in delayed queue can be very high based on the use case.
 */
public abstract class RqueueConfig {

  /**
   * This is more for testing features where scheduler is not started automatically, based on the
   * messages from Redis PUB/SUB channel tasks are executed.
   */
  @Value("${auto.start.scheduler:true}")
  private boolean autoStartScheduler;

  @Value("${delayed.queue.thread.pool.size:5}")
  private int delayedQueueSchedulerPoolSize;

  @Value("${processing.queue.thread.pool.size:1}")
  private int processingQueueSchedulerPoolSize;

  @Autowired(required = false)
  protected final SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
      new SimpleRqueueListenerContainerFactory();

  @Autowired protected BeanFactory beanFactory;

  /**
   * Get redis connection factory either from listener container factory or from bean factory. 1st
   * priority is given to container factory. This redis connection factory is used to connect to
   * Database for different ops.
   *
   * @return {@link RedisConnectionFactory} object
   */
  protected RedisConnectionFactory getRedisConnectionFactory() {
    if (simpleRqueueListenerContainerFactory.getRedisConnectionFactory() == null) {
      simpleRqueueListenerContainerFactory.setRedisConnectionFactory(
          beanFactory.getBean(RedisConnectionFactory.class));
    }
    return simpleRqueueListenerContainerFactory.getRedisConnectionFactory();
  }

  /**
   * Get Rqueue message template either from listener container factory or create new one. 1st
   * priority is given to container factory. Message template is used to serialize message and
   * sending message to Redis.
   *
   * @return {@link RqueueMessageTemplate} object
   */
  protected RqueueMessageTemplate getMessageTemplate(RedisConnectionFactory connectionFactory) {
    if (simpleRqueueListenerContainerFactory.getRqueueMessageTemplate() != null) {
      return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
    }
    simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(
        new RqueueMessageTemplate(connectionFactory));
    return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
  }

  /**
   * This scheduler is used to pull messages from delayed queue to their respective queue.
   * Internally it moves messages from ZSET to LIST based on the priority and current time.
   *
   * @return {@link MessageScheduler} object
   */
  @Bean
  public MessageScheduler messageScheduler() {
    return new MessageScheduler(
        getRedisTemplate(getRedisConnectionFactory()),
        delayedQueueSchedulerPoolSize,
        autoStartScheduler);
  }

  /**
   * This scheduler is used to pull messages from processing queue to their respective queue.
   * Internally it moves messages from ZSET to LIST based on the priority and current time.
   *
   * @return {@link MessageScheduler} object
   */
  @Bean
  public ProcessingMessageScheduler processingMessageScheduler() {
    return new ProcessingMessageScheduler(
        getRedisTemplate(getRedisConnectionFactory()),
        processingQueueSchedulerPoolSize,
        autoStartScheduler);
  }
}
