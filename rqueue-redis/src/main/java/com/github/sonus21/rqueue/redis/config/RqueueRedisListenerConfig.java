/*
 * Copyright (c) 2026 Sonu Kumar
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
package com.github.sonus21.rqueue.redis.config;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RedisBackendCondition;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueInternalPubSubChannel;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.RqueueRedisListenerContainerFactory;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.core.spi.redis.RedisMessageBroker;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetricsProvider;
import com.github.sonus21.rqueue.redis.core.ProcessingQueueMessageScheduler;
import com.github.sonus21.rqueue.redis.core.ScheduledQueueMessageScheduler;
import com.github.sonus21.rqueue.redis.dao.RqueueStringDaoImpl;
import com.github.sonus21.rqueue.redis.lock.RqueueRedisLock;
import com.github.sonus21.rqueue.redis.metrics.RedisRqueueQueueMetricsProvider;
import com.github.sonus21.rqueue.redis.repository.RedisMessageBrowsingRepository;
import com.github.sonus21.rqueue.redis.worker.RedisWorkerRegistryStore;
import com.github.sonus21.rqueue.repository.MessageBrowsingRepository;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.worker.RqueueWorkerRegistry;
import com.github.sonus21.rqueue.worker.RqueueWorkerRegistryImpl;
import com.github.sonus21.rqueue.worker.WorkerRegistryStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

/**
 * Redis-conditional bean wiring extracted from {@code RqueueListenerBaseConfig} so that the
 * Redis-only impl classes can live in {@code rqueue-redis} without forcing {@code rqueue-core} to
 * depend on this module. {@link com.github.sonus21.rqueue.spring.RqueueListenerConfig} (non-Boot)
 * and {@link com.github.sonus21.rqueue.spring.boot.RqueueListenerAutoConfig} (Boot) each
 * {@code @Import} this configuration, so the Redis @Beans are registered exactly where they used to
 * be.
 *
 * <p>Mirrors the role {@code RqueueNatsAutoConfig} plays for the NATS backend.
 */
@Configuration
@Conditional(RedisBackendCondition.class)
@ComponentScan({
  "com.github.sonus21.rqueue.redis",
})
public class RqueueRedisListenerConfig {

  @Bean
  @Conditional(RedisBackendCondition.class)
  public MessageBroker redisMessageBroker(RqueueMessageTemplate rqueueMessageTemplate) {
    return new RedisMessageBroker(rqueueMessageTemplate);
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueStringDao rqueueStringDao(RqueueConfig rqueueConfig) {
    return new RqueueStringDaoImpl(rqueueConfig);
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RedisTemplate<String, Long> rqueueRedisLongTemplate(RqueueConfig rqueueConfig) {
    return RedisUtils.getRedisTemplate(rqueueConfig.getConnectionFactory());
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory() {
    return new RqueueRedisListenerContainerFactory();
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueRedisTemplate<String> stringRqueueRedisTemplate(RqueueConfig rqueueConfig) {
    return new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueInternalPubSubChannel rqueueInternalPubSubChannel(
      RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory,
      RqueueMessageListenerContainer rqueueMessageListenerContainer,
      RqueueConfig rqueueConfig,
      RqueueBeanProvider rqueueBeanProvider,
      @Qualifier("stringRqueueRedisTemplate")
          RqueueRedisTemplate<String> stringRqueueRedisTemplate) {
    return new RqueueInternalPubSubChannel(
        rqueueRedisListenerContainerFactory,
        rqueueMessageListenerContainer,
        rqueueConfig,
        stringRqueueRedisTemplate,
        rqueueBeanProvider);
  }

  /**
   * Pulls due delayed messages from the per-queue ZSET back onto the ready LIST. Redis-only; NATS
   * uses JetStream's native redelivery instead.
   */
  @Bean
  @Conditional(RedisBackendCondition.class)
  public ScheduledQueueMessageScheduler scheduledMessageScheduler() {
    return new ScheduledQueueMessageScheduler();
  }

  /**
   * Re-queues messages whose ack-window expired without explicit ack. Redis-only; the equivalent on
   * NATS is the consumer's {@code AckWait} timer.
   */
  @Bean
  @Conditional(RedisBackendCondition.class)
  public ProcessingQueueMessageScheduler processingMessageScheduler() {
    return new ProcessingQueueMessageScheduler();
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public WorkerRegistryStore redisWorkerRegistryStore(RqueueConfig rqueueConfig) {
    return new RedisWorkerRegistryStore(rqueueConfig);
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public MessageBrowsingRepository messageBrowsingRepository(
      @Qualifier("stringRqueueRedisTemplate")
          RqueueRedisTemplate<String> stringRqueueRedisTemplate) {
    return new RedisMessageBrowsingRepository(stringRqueueRedisTemplate);
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueWorkerRegistry rqueueWorkerRegistry(
      RqueueConfig rqueueConfig, WorkerRegistryStore workerRegistryStore) {
    return new RqueueWorkerRegistryImpl(rqueueConfig, workerRegistryStore);
  }

  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueLockManager rqueueLockManager(RqueueStringDao rqueueStringDao) {
    return new RqueueRedisLock(rqueueStringDao);
  }

  /**
   * Backend-agnostic queue-depth gauge source consumed by
   * {@link com.github.sonus21.rqueue.metrics.RqueueMetrics}. Reuses the existing
   * {@code stringRqueueRedisTemplate} bean so we don't add a new connection-bound dependency.
   */
  @Bean
  @Conditional(RedisBackendCondition.class)
  public RqueueQueueMetricsProvider rqueueQueueMetricsProvider(
      @Qualifier("stringRqueueRedisTemplate")
          RqueueRedisTemplate<String> stringRqueueRedisTemplate) {
    return new RedisRqueueQueueMetricsProvider(stringRqueueRedisTemplate);
  }
}
