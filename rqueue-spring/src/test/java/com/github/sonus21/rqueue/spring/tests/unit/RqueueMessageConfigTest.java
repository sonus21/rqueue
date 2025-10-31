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

package com.github.sonus21.rqueue.spring.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.spring.RqueueListenerConfig;
import com.github.sonus21.rqueue.spring.tests.SpringUnitTest;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

@SpringUnitTest
class RqueueMessageConfigTest extends TestBase {

  private final List<MessageConverter> messageConverterList = new ArrayList<>();
  @Mock
  RqueueMessageHandler rqueueMessageHandler;
  @Mock
  private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;
  @Mock
  private BeanFactory beanFactory;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RedisConnectionFactory redisConnectionFactory;
  @InjectMocks
  private RqueueListenerConfig rqueueMessageConfig;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    messageConverterList.clear();
    messageConverterList.add(new GenericMessageConverter());
  }

  @Test
  void rqueueMessageHandlerDefaultCreation() throws IllegalAccessException {
    FieldUtils.writeField(
        rqueueMessageConfig,
        "messageConverterProviderClass",
        "com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider",
        true);
    assertNotNull(rqueueMessageConfig.rqueueMessageHandler());
  }

  @Test
  void rqueueMessageHandlerReused() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageHandler(rqueueMessageHandler);
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    FieldUtils.writeField(
        messageConfig,
        "messageConverterProviderClass",
        "com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider",
        true);
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertEquals(rqueueMessageHandler.hashCode(), messageConfig.rqueueMessageHandler().hashCode());
  }

  @Test
  void rqueueMessageListenerContainer() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setMessageConverterProvider(new DefaultMessageConverterProvider());
    factory.setRedisConnectionFactory(redisConnectionFactory);
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    FieldUtils.writeField(
        messageConfig,
        "messageConverterProviderClass",
        "com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider",
        true);
    messageConfig.rqueueMessageListenerContainer(rqueueMessageHandler);
    assertEquals(factory.getRqueueMessageHandler(null).hashCode(), rqueueMessageHandler.hashCode());
  }

  @Test
  void rqueueMessageSenderWithMessageTemplate() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageTemplate(rqueueMessageTemplate);
    doReturn(new DefaultRqueueMessageConverter()).when(rqueueMessageHandler).getMessageConverter();
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertNotNull(messageConfig.rqueueMessageEnqueuer(rqueueMessageHandler, rqueueMessageTemplate));
    assertEquals(factory.getRqueueMessageTemplate().hashCode(), rqueueMessageTemplate.hashCode());
  }
}
