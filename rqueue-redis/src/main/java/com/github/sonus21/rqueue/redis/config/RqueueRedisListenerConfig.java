/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RedisBackendCondition;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueInternalPubSubChannel;
import com.github.sonus21.rqueue.core.RqueueRedisListenerContainerFactory;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.redis.dao.RqueueStringDaoImpl;
import com.github.sonus21.rqueue.utils.RedisUtils;
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
 * {@code @Import} this configuration, so the Redis @Beans are registered exactly where they used
 * to be.
 *
 * <p>Mirrors the role {@code RqueueNatsAutoConfig} plays for the NATS backend.
 */
@Configuration
@Conditional(RedisBackendCondition.class)
@ComponentScan("com.github.sonus21.rqueue.redis.dao")
public class RqueueRedisListenerConfig {

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
}
