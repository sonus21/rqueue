/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.RedisUtils.getRedisTemplate;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.common.impl.RqueueLockManagerImpl;
import com.github.sonus21.rqueue.converter.MessageConverterProvider;
import com.github.sonus21.rqueue.core.ProcessingQueueMessageScheduler;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueInternalPubSubChannel;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.RqueueRedisListenerContainerFactory;
import com.github.sonus21.rqueue.core.ScheduledQueueMessageScheduler;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.impl.RqueueStringDaoImpl;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetrics;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.condition.ReactiveEnabled;
import com.github.sonus21.rqueue.utils.pebble.ResourceLoader;
import com.github.sonus21.rqueue.utils.pebble.RqueuePebbleExtension;
import io.pebbletemplates.pebble.PebbleEngine;
import io.pebbletemplates.spring.extension.SpringExtension;
import io.pebbletemplates.spring.reactive.PebbleReactiveViewResolver;
import io.pebbletemplates.spring.servlet.PebbleViewResolver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.servlet.ViewResolver;

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
  private static final String TEMPLATE_DIR = "templates/rqueue/";
  private static final String TEMPLATE_SUFFIX = ".html";

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
  @Bean
  public RqueueConfig rqueueConfig(
      ConfigurableBeanFactory beanFactory,
      @Value("${rqueue.version.key:__rq::version}") String versionKey,
      @Value("${rqueue.db.version:}") Integer dbVersion) {
    boolean sharedConnection = false;
    if (simpleRqueueListenerContainerFactory.getRedisConnectionFactory() == null) {
      sharedConnection = true;
      simpleRqueueListenerContainerFactory.setRedisConnectionFactory(
          beanFactory.getBean(RedisConnectionFactory.class));
    }
    if (reactiveEnabled
        && simpleRqueueListenerContainerFactory.getReactiveRedisConnectionFactory() == null) {
      sharedConnection = true;
      simpleRqueueListenerContainerFactory.setReactiveRedisConnectionFactory(
          beanFactory.getBean(ReactiveRedisConnectionFactory.class));
    }
    RedisConnectionFactory connectionFactory =
        simpleRqueueListenerContainerFactory.getRedisConnectionFactory();
    RqueueRedisTemplate<Integer> rqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    int version;
    if (dbVersion == null) {
      version = RedisUtils.updateAndGetVersion(rqueueRedisTemplate, versionKey, MAX_DB_VERSION);
    } else if (dbVersion >= 1 && dbVersion <= MAX_DB_VERSION) {
      RedisUtils.setVersion(rqueueRedisTemplate, versionKey, dbVersion);
      version = dbVersion;
    } else {
      throw new IllegalStateException("Rqueue db version '" + dbVersion + "' is not correct");
    }
    return new RqueueConfig(
        connectionFactory,
        simpleRqueueListenerContainerFactory.getReactiveRedisConnectionFactory(),
        sharedConnection,
        version);
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
        new RqueueMessageTemplateImpl(
            rqueueConfig.getConnectionFactory(), rqueueConfig.getReactiveRedisConnectionFactory()));
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
   * This scheduler is used to pull messages from a scheduled queue to their respective queue.
   * Internally it moves messages from ZSET to LIST based on the priority and current time.
   *
   * @return {@link ScheduledQueueMessageScheduler} object
   */
  @Bean
  public ScheduledQueueMessageScheduler scheduledMessageScheduler() {
    return new ScheduledQueueMessageScheduler();
  }

  /**
   * This scheduler is used to pull messages from processing queue to their respective queue.
   * Internally it moves messages from ZSET to LIST based on the priority and current time.
   *
   * @return {@link ProcessingQueueMessageScheduler} object
   */
  @Bean
  public ProcessingQueueMessageScheduler processingMessageScheduler() {
    return new ProcessingQueueMessageScheduler();
  }

  @Bean
  public RqueueRedisTemplate<String> stringRqueueRedisTemplate(RqueueConfig rqueueConfig) {
    return new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Bean
  public RqueueStringDao rqueueStringDao(RqueueConfig rqueueConfig) {
    return new RqueueStringDaoImpl(rqueueConfig);
  }

  @Bean
  public RqueueLockManager rqueueLockManager(RqueueStringDao rqueueStringDao) {
    return new RqueueLockManagerImpl(rqueueStringDao);
  }

  private PebbleEngine createPebbleEngine() {
    ResourceLoader loader = new ResourceLoader();
    loader.setPrefix(TEMPLATE_DIR);
    loader.setSuffix(TEMPLATE_SUFFIX);
    return new PebbleEngine.Builder()
        .extension(new RqueuePebbleExtension(), new SpringExtension(null))
        .loader(loader)
        .build();
  }

  @Bean
  public ViewResolver rqueueViewResolver() {
    PebbleViewResolver resolver = new PebbleViewResolver(createPebbleEngine());
    resolver.setPrefix(TEMPLATE_DIR);
    resolver.setSuffix(TEMPLATE_SUFFIX);
    return resolver;
  }

  @Bean
  @Conditional(ReactiveEnabled.class)
  public org.springframework.web.reactive.result.view.ViewResolver reactiveRqueueViewResolver() {
    PebbleReactiveViewResolver resolver = new PebbleReactiveViewResolver(createPebbleEngine());
    resolver.setPrefix(TEMPLATE_DIR);
    resolver.setSuffix(TEMPLATE_SUFFIX);
    return resolver;
  }

  @Bean
  public RqueueQueueMetrics rqueueQueueMetrics(
      RqueueRedisTemplate<String> stringRqueueRedisTemplate) {
    return new RqueueQueueMetrics(stringRqueueRedisTemplate);
  }

  @Bean
  public RqueueBeanProvider rqueueBeanProvider() {
    return new RqueueBeanProvider();
  }

  @Bean
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
