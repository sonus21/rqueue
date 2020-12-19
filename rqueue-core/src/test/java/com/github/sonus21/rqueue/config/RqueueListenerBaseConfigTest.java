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

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
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
  @Mock private RedisConnection redisConnection;
  @Mock private ConfigurableBeanFactory beanFactory;
  @Mock private RedisConnectionFactory redisConnectionFactory;
  @Mock private RqueueMessageTemplate rqueueMessageTemplate;

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
  void testRqueueConfigSetConnectionFactoryFromBeanFactory() throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();

    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    assertNotNull(rqueueSystemConfig.rqueueConfig(beanFactory, "__rq::version", null));
    assertNotNull(factory.getRedisConnectionFactory());
  }

  @Test
  void testRqueueConfigSetConnectionFactoryFromBeanFactoryWithDbVersion()
      throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    assertNotNull(rqueueSystemConfig.rqueueConfig(beanFactory, "__rq::version", 1));
    assertNotNull(factory.getRedisConnectionFactory());
  }

  @Test
  void testRqueueConfigInvalidDbVersion() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    doReturn(redisConnectionFactory).when(beanFactory).getBean(RedisConnectionFactory.class);
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> rqueueSystemConfig.rqueueConfig(beanFactory, "__rq::version", 3));
  }

  @Test
  void testRqueueConfigDoesNotChangeConnectionFactory() throws IllegalAccessException {
    doReturn(redisConnection).when(redisConnectionFactory).getConnection();

    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRedisConnectionFactory(redisConnectionFactory);
    RqueueListenerConfig rqueueSystemConfig = createConfig(factory);
    assertNotNull(rqueueSystemConfig.rqueueConfig(beanFactory, "__rq::version", null));
    assertNotNull(factory.getRedisConnectionFactory());
    assertEquals(redisConnectionFactory, factory.getRedisConnectionFactory());
    verify(beanFactory, times(0)).getBean(RedisConnectionFactory.class);
  }

  @Test
  void testGetMessageTemplateUseFromFactory() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueListenerConfig = createConfig(factory);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, false, 1);
    factory.setRqueueMessageTemplate(rqueueMessageTemplate);
    assertEquals(rqueueMessageTemplate, rqueueListenerConfig.getMessageTemplate(rqueueConfig));
  }

  @Test
  void testGetMessageTemplateSetTemplateInFactory() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueListenerConfig rqueueListenerConfig = createConfig(factory);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, false, 1);
    RqueueMessageTemplate template = rqueueListenerConfig.getMessageTemplate(rqueueConfig);
    assertNotNull(template);
    assertEquals(template, factory.getRqueueMessageTemplate());
  }

  private class RqueueListenerConfig extends RqueueListenerBaseConfig {}
}
