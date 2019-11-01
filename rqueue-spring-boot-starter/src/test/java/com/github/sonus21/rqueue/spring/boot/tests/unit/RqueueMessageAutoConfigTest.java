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

package com.github.sonus21.rqueue.spring.boot.tests.unit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.RqueueMessageAutoConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageAutoConfigTest {
  @Mock private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;
  @Mock private BeanFactory beanFactory;
  private List<MessageConverter> messageConverterList = new ArrayList<>();

  @InjectMocks private RqueueMessageAutoConfig rqueueMessageAutoConfig;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    messageConverterList.clear();
    messageConverterList.add(new GenericMessageConverter());
  }

  @Test
  public void rqueueMessageHandlerDefaultCreation() {
    assertNotNull(rqueueMessageAutoConfig.rqueueMessageHandler());
  }

  @Test
  public void rqueueMessageHandlerReused() throws IllegalAccessException {
    RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRqueueMessageHandler(rqueueMessageHandler);
    RqueueMessageAutoConfig messageAutoConfig = new RqueueMessageAutoConfig();
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    assertEquals(
        rqueueMessageHandler.hashCode(), messageAutoConfig.rqueueMessageHandler().hashCode());
  }

  @Test
  public void rqueueMessageHandlerCreatedWithMessageConverters() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setMessageConverters(messageConverterList);
    RqueueMessageAutoConfig messageAutoConfig = new RqueueMessageAutoConfig();
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    RqueueMessageHandler messageHandler = messageAutoConfig.rqueueMessageHandler();
    assertEquals(messageConverterList.get(0), messageHandler.getMessageConverters().get(0));
  }

  @Test
  public void rqueueMessageListenerContainer() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueMessageAutoConfig messageAutoConfig = new RqueueMessageAutoConfig();
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    FieldUtils.writeField(messageAutoConfig, "beanFactory", beanFactory, true);
    RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
    doReturn(mock(RedisConnectionFactory.class))
        .when(beanFactory)
        .getBean(RedisConnectionFactory.class);
    messageAutoConfig.rqueueMessageListenerContainer(messageHandler);
    assertEquals(factory.getRqueueMessageHandler().hashCode(), messageHandler.hashCode());
  }

  @Test
  public void rqueueMessageSenderWithoutMessageTemplate() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueMessageAutoConfig messageAutoConfig = new RqueueMessageAutoConfig();
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    FieldUtils.writeField(messageAutoConfig, "beanFactory", beanFactory, true);

    doReturn(mock(RedisConnectionFactory.class))
        .when(beanFactory)
        .getBean(RedisConnectionFactory.class);
    assertNotNull(messageAutoConfig.rqueueMessageSender());
    assertNotNull(factory.getRqueueMessageTemplate());
  }

  @Test
  public void rqueueMessageSenderWithMessageTemplate() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
    factory.setRqueueMessageTemplate(messageTemplate);
    RqueueMessageAutoConfig messageAutoConfig = new RqueueMessageAutoConfig();
    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    FieldUtils.writeField(messageAutoConfig, "beanFactory", beanFactory, true);
    doReturn(mock(RedisConnectionFactory.class))
        .when(beanFactory)
        .getBean(RedisConnectionFactory.class);
    assertNotNull(messageAutoConfig.rqueueMessageSender());
    assertEquals(factory.getRqueueMessageTemplate().hashCode(), messageTemplate.hashCode());
  }

  @Test
  public void rqueueMessageSenderWithMessageConverters() throws IllegalAccessException {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageConverter messageConverter = new GenericMessageConverter();
    RqueueMessageAutoConfig messageAutoConfig = new RqueueMessageAutoConfig();
    factory.setMessageConverters(Collections.singletonList(messageConverter));

    FieldUtils.writeField(messageAutoConfig, "simpleRqueueListenerContainerFactory", factory, true);
    FieldUtils.writeField(messageAutoConfig, "beanFactory", beanFactory, true);
    doReturn(mock(RedisConnectionFactory.class))
        .when(beanFactory)
        .getBean(RedisConnectionFactory.class);
    assertNotNull(messageAutoConfig.rqueueMessageSender());
    RqueueMessageSender messageSender = messageAutoConfig.rqueueMessageSender();
    boolean messageConverterIsConfigured = false;
    for (MessageConverter converter : messageSender.getMessageConverters()) {
      messageConverterIsConfigured =
          messageConverterIsConfigured || converter.hashCode() == messageConverter.hashCode();
    }
    assertTrue(messageConverterIsConfigured);
  }
}
