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

package com.github.sonus21.rqueue.spring.tests.unit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.spring.RqueueListenerConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

@ExtendWith(MockitoExtension.class)
public class RqueueMessageConfigTest {
  @Mock private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;
  @Mock private BeanFactory beanFactory;
  @Mock private RqueueMessageTemplate rqueueMessageTemplate;
  @InjectMocks private RqueueListenerConfig rqueueMessageConfig;
  private List<MessageConverter> messageConverterList = new ArrayList<>();

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    messageConverterList.clear();
    messageConverterList.add(new GenericMessageConverter());
  }

  @Test
  public void rqueueMessageHandlerDefaultCreation() {
    assertNotNull(rqueueMessageConfig.rqueueMessageHandler());
  }

  @Test
  public void rqueueMessageHandlerReused() throws IllegalAccessException {
    RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageHandler(rqueueMessageHandler);
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertEquals(rqueueMessageHandler.hashCode(), messageConfig.rqueueMessageHandler().hashCode());
  }

  @Test
  public void rqueueMessageListenerContainer() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRedisConnectionFactory(mock(RedisConnectionFactory.class));
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
    messageConfig.rqueueMessageListenerContainer(messageHandler);
    assertEquals(factory.getRqueueMessageHandler().hashCode(), messageHandler.hashCode());
  }

  @Test
  public void rqueueMessageSenderWithMessageTemplate() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
    factory.setRqueueMessageTemplate(messageTemplate);
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertNotNull(messageConfig.rqueueMessageSender(messageTemplate));
    assertEquals(factory.getRqueueMessageTemplate().hashCode(), messageTemplate.hashCode());
  }

  @Test
  public void rqueueMessageSenderWithMessageConverters() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageConverter messageConverter = new DefaultRqueueMessageConverter();
    RqueueListenerConfig messageConfig = new RqueueListenerConfig();
    factory.setMessageConverters(Collections.singletonList(messageConverter));
    FieldUtils.writeField(messageConfig, "simpleRqueueListenerContainerFactory", factory, true);
    factory.setRedisConnectionFactory(mock(RedisConnectionFactory.class));
    assertNotNull(messageConfig.rqueueMessageSender(rqueueMessageTemplate));
    RqueueMessageSender messageSender = messageConfig.rqueueMessageSender(rqueueMessageTemplate);
    boolean messageConverterIsConfigured = false;
    for (MessageConverter converter : messageSender.getMessageConverters()) {
      messageConverterIsConfigured =
          messageConverterIsConfigured || converter.hashCode() == messageConverter.hashCode();
    }
    assertTrue(messageConverterIsConfigured);
  }
}
