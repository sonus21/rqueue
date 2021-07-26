/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.spring.boot.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.converter.MessageConverterProvider;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.spring.boot.RqueueListenerAutoConfig;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootUnitTest;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

@SpringBootUnitTest
class RqueueListenerAutoConfigTest extends TestBase {
  @Mock private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;
  @Mock private BeanFactory beanFactory;
  @Mock private RqueueMessageTemplate messageTemplate;
  @Mock private RqueueMessageHandler rqueueMessageHandler;
  @Mock private RedisConnectionFactory redisConnectionFactory;
  @InjectMocks private RqueueListenerAutoConfig rqueueMessageAutoConfig;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    FieldUtils.writeField(
        rqueueMessageAutoConfig,
        "messageConverterProviderClass",
        "com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider",
        true);
  }

  @Test
  void rqueueMessageHandlerDefaultCreation()
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    assertNotNull(rqueueMessageAutoConfig.rqueueMessageHandler());
  }

  @Test
  void rqueueMessageHandlerReused()
      throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageHandler(rqueueMessageHandler);
    RqueueListenerAutoConfig messageAutoConfig = new RqueueListenerAutoConfig();
    FieldUtils.writeField(
        messageAutoConfig,
        "messageConverterProviderClass",
        "com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider",
        true);
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertEquals(
        rqueueMessageHandler.hashCode(), messageAutoConfig.rqueueMessageHandler().hashCode());
  }

  @Test
  void rqueueMessageListenerContainer()
      throws IllegalAccessException, ClassNotFoundException, InstantiationException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRedisConnectionFactory(redisConnectionFactory);
    RqueueListenerAutoConfig messageAutoConfig = new RqueueListenerAutoConfig();
    FieldUtils.writeField(
        messageAutoConfig,
        "messageConverterProviderClass",
        "com.github.sonus21.rqueue.converter.DefaultMessageConverterProvider",
        true);
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    messageAutoConfig.rqueueMessageListenerContainer(rqueueMessageHandler);
    assertEquals(factory.getRqueueMessageHandler(null).hashCode(), rqueueMessageHandler.hashCode());
  }

  @Test
  void rqueueMessageSenderWithMessageTemplate() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageTemplate(messageTemplate);
    doReturn(new DefaultRqueueMessageConverter()).when(rqueueMessageHandler).getMessageConverter();
    RqueueListenerAutoConfig messageAutoConfig = new RqueueListenerAutoConfig();
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertNotNull(messageAutoConfig.rqueueMessageSender(rqueueMessageHandler, messageTemplate));
    assertEquals(factory.getRqueueMessageTemplate().hashCode(), messageTemplate.hashCode());
  }

  @Test
  void rqueueMessageSenderWithMessageConverters() throws IllegalAccessException {
    MessageConverter messageConverter = new GenericMessageConverter();
    MessageConverterProvider messageConverterProvider = () -> messageConverter;
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setMessageConverterProvider(messageConverterProvider);
    RqueueListenerAutoConfig messageAutoConfig = new RqueueListenerAutoConfig();
    factory.setRqueueMessageTemplate(messageTemplate);
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    doReturn(new DefaultRqueueMessageConverter()).when(rqueueMessageHandler).getMessageConverter();
    assertNotNull(messageAutoConfig.rqueueMessageSender(rqueueMessageHandler, messageTemplate));
    RqueueMessageSender messageSender =
        messageAutoConfig.rqueueMessageSender(rqueueMessageHandler, messageTemplate);
    boolean messageConverterIsConfigured = false;
    for (MessageConverter converter : messageSender.getMessageConverters()) {
      messageConverterIsConfigured =
          messageConverterIsConfigured || converter.hashCode() == messageConverter.hashCode();
    }
    assertTrue(messageConverterIsConfigured);
  }
}
