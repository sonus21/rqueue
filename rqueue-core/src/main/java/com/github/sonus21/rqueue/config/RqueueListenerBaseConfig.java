/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.config;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.converter.MessageConverterProvider;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueMessageIdGenerator;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.core.impl.UuidV4RqueueMessageIdGenerator;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.condition.MissingRqueueMessageIdGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * This is a base configuration class for Rqueue, that is used in Spring and Spring boot Rqueue libs
 * for configurations. This class creates required beans to work Rqueue library.
 *
 * <p>It internally maintains two types of scheduled tasks for different functionality, for
 * scheduled queue messages have to be moved from ZSET to LIST, in other case to at least once
 * message delivery guarantee, messages have to be moved from ZSET to LIST again, we expect very
 * small number of messages in processing queue. Reason being we delete messages once it's consumed,
 * but due to failure in listeners message might not be removed, whereas message in a scheduled
 * queue can be very high based on the use case.
 */
public abstract class RqueueListenerBaseConfig {

  public static final int MAX_DB_VERSION = 2;

  @Autowired(required = false)
  protected final SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
      new SimpleRqueueListenerContainerFactory();

  @Value("${rqueue.reactive.enabled:false}")
  protected boolean reactiveEnabled;

  @Value(
      "${rqueue.message.converter.provider.class:com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider}")
  private String messageConverterProviderClass;

  protected MessageConverterProvider getMessageConverterProvider() {
    try {
      Class<?> c =
          Thread.currentThread().getContextClassLoader().loadClass(messageConverterProviderClass);
      Object messageProvider = c.newInstance();
      if (messageProvider instanceof MessageConverterProvider) {
        return (MessageConverterProvider) messageProvider;
      }
      throw new IllegalStateException(
          "configured message converter is not of type MessageConverterProvider, type: '"
              + messageConverterProviderClass
              + "'",
          new Exception());
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new IllegalStateException(
          "MessageConverterProvider class '" + messageConverterProviderClass + "' loading failed ",
          e);
    }
  }

  /**
   * Create Rqueue configuration bean either from listener container factory or from bean factory.
   * 1st priority is given to container factory. This redis connection factory is used to connect to
   * Database for different ops.
   *
   * @param beanFactory configurable bean factory
   * @param versionKey  Rqueue db version key
   * @param dbVersion   database version
   * @return {@link RedisConnectionFactory} object.
   */
  /**
   * Backwards-compatible overload preserved for callers (notably existing tests) that constructed
   * an {@code RqueueConfig} without specifying a backend. Delegates with {@link Backend#REDIS}.
   */
  public RqueueConfig rqueueConfig(
      ConfigurableBeanFactory beanFactory, String versionKey, Integer dbVersion) {
    return rqueueConfig(beanFactory, Backend.REDIS, versionKey, dbVersion);
  }

  @Bean
  public RqueueConfig rqueueConfig(
      ConfigurableBeanFactory beanFactory,
      @Value("${rqueue.backend:REDIS}") Backend backend,
      @Value("${rqueue.version.key:__rq::version}") String versionKey,
      @Value("${rqueue.db.version:}") Integer dbVersion) {
    boolean sharedConnection = false;
    RedisConnectionFactory connectionFactory =
        simpleRqueueListenerContainerFactory.getRedisConnectionFactory();
    if (backend == Backend.REDIS) {
      if (connectionFactory == null) {
        sharedConnection = true;
        connectionFactory = beanFactory.getBean(RedisConnectionFactory.class);
        simpleRqueueListenerContainerFactory.setRedisConnectionFactory(connectionFactory);
      }
      if (reactiveEnabled
          && simpleRqueueListenerContainerFactory.getReactiveRedisConnectionFactory() == null) {
        sharedConnection = true;
        simpleRqueueListenerContainerFactory.setReactiveRedisConnectionFactory(
            beanFactory.getBean(ReactiveRedisConnectionFactory.class));
      }
    }
    int version;
    if (backend == Backend.REDIS) {
      RqueueRedisTemplate<Integer> rqueueRedisTemplate =
          new RqueueRedisTemplate<>(connectionFactory);
      if (dbVersion == null) {
        version = RedisUtils.updateAndGetVersion(rqueueRedisTemplate, versionKey, MAX_DB_VERSION);
      } else if (dbVersion >= 1 && dbVersion <= MAX_DB_VERSION) {
        RedisUtils.setVersion(rqueueRedisTemplate, versionKey, dbVersion);
        version = dbVersion;
      } else {
        throw new IllegalStateException("Rqueue db version '" + dbVersion + "' is not correct");
      }
    } else {
      // Non-Redis backend (e.g. NATS): the on-Redis db-version negotiation does not apply.
      version = (dbVersion != null && dbVersion >= 1 && dbVersion <= MAX_DB_VERSION)
          ? dbVersion
          : MAX_DB_VERSION;
    }
    RqueueConfig config = new RqueueConfig(
        connectionFactory,
        simpleRqueueListenerContainerFactory.getReactiveRedisConnectionFactory(),
        sharedConnection,
        version);
    config.setBackend(backend);
    return config;
  }

  @Bean
  public RqueueWebConfig rqueueWebConfig() {
    return new RqueueWebConfig();
  }

  @Bean
  @Conditional(MissingRqueueMessageIdGenerator.class)
  public RqueueMessageIdGenerator rqueueMessageIdGenerator() {
    return new UuidV4RqueueMessageIdGenerator();
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
    simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(new RqueueMessageTemplateImpl(
        rqueueConfig.getConnectionFactory(), rqueueConfig.getReactiveRedisConnectionFactory()));
    return simpleRqueueListenerContainerFactory.getRqueueMessageTemplate();
  }

  @Bean
  public RqueueBeanProvider rqueueBeanProvider() {
    return new RqueueBeanProvider();
  }
}
