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

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@CoreUnitTest
class RqueueListenerBaseConfigTest extends TestBase {

  private final String versionKey = "__rq::version";
  @Mock
  private RedisConnection redisConnection;
  @Mock
  private ConfigurableBeanFactory beanFactory;
  @Mock
  private RedisConnectionFactory redisConnectionFactory;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
  }

  private RqueueListenerConfig createConfig(SimpleRqueueListenerContainerFactory factory)
      throws IllegalAccessException {
    RqueueListenerConfig rqueueSystemConfig = new RqueueListenerConfig();
    writeField(rqueueSystemConfig, "simpleRqueueListenerContainerFactory", factory, true);
    return rqueueSystemConfig;
  }

  @Test
  void rqueueConfigSetConnectionFactoryFromBeanFactoryNoExistingData()
      throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    doReturn(Collections.singletonList(0L)).when(redisConnection).closePipeline();
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    RqueueConfig rqueueConfig = rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, null);
    assertNotNull(rqueueConfig);
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(2, rqueueConfig.getDbVersion());
    verify(redisConnection, times(1))
        .set(
            versionKey.getBytes(StandardCharsets.UTF_8),
            String.valueOf(2).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void rqueueConfigSetConnectionFactoryFromBeanFactoryExistingData() throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    doReturn(Collections.singletonList(2L)).when(redisConnection).closePipeline();
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    RqueueConfig rqueueConfig = rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, null);
    assertNotNull(rqueueConfig);
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(1, rqueueConfig.getDbVersion());
    verify(redisConnection, times(1))
        .set(
            versionKey.getBytes(StandardCharsets.UTF_8),
            String.valueOf(1).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void rqueueConfigSetConnectionFactoryFromBeanFactoryWithDifferentValue()
      throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    doReturn(String.valueOf(1).getBytes(StandardCharsets.UTF_8)).when(redisConnection).get(any());
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    RqueueConfig rqueueConfig = rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, null);
    assertNotNull(rqueueConfig);
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(1, rqueueConfig.getDbVersion());
    verify(redisConnection, times(0)).set(any(), any());
  }

  @Test
  void rqueueConfigSetConnectionFactoryFromBeanFactoryWithLatestVersion()
      throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    doReturn(String.valueOf(2).getBytes(StandardCharsets.UTF_8)).when(redisConnection).get(any());
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    RqueueConfig rqueueConfig = rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, null);
    assertNotNull(rqueueConfig);
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(2, rqueueConfig.getDbVersion());
    verify(redisConnection, times(0)).set(any(), any());
  }

  @Test
  void rqueueConfigSetConnectionFactoryFromBeanFactoryWithDbVersion()
      throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    RqueueConfig rqueueConfig = rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, 1);
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(1, rqueueConfig.getDbVersion());
    verify(redisConnection, times(1)).set(any(), any());
  }

  @Test
  void rqueueConfigInvalidDbVersion() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, 3));
  }

  @Test
  void rqueueConfigDoesNotChangeConnectionFactory() throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    doReturn(String.valueOf(2).getBytes(StandardCharsets.UTF_8)).when(redisConnection).get(any());
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRedisConnectionFactory(redisConnectionFactory);
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    assertNotNull(rqueueSystemConfig.rqueueConfig(beanFactory, versionKey, null));
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(redisConnectionFactory, factory.getRedisConnectionFactory());
    verify(beanFactory, times(0)).getBean(RedisConnectionFactory.class);
  }

  @Test
  void getMessageTemplateUseFromFactory() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueListenerConfig = createConfig(factory);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, false, 1);
    factory.setRqueueMessageTemplate(rqueueMessageTemplate);
    assertEquals(rqueueMessageTemplate, rqueueListenerConfig.getMessageTemplate(rqueueConfig));
  }

  @Test
  void getMessageTemplateSetTemplateInFactory() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueListenerConfig = createConfig(factory);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, false, 1);
    RqueueMessageTemplate template = rqueueListenerConfig.getMessageTemplate(rqueueConfig);
    assertNotNull(template);
    assertEquals(template, factory.getRqueueMessageTemplate());
  }

  private class RqueueListenerConfig extends RqueueListenerBaseConfig {

  }
}
