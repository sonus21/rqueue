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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SimpleRqueueListenerContainerFactoryTest {
  private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;
  private AsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

  @Before
  public void init() {
    simpleRqueueListenerContainerFactory = new SimpleRqueueListenerContainerFactory();
  }

  @Test(expected = IllegalArgumentException.class)
  public void setTaskExecutor() {
    simpleRqueueListenerContainerFactory.setTaskExecutor(null);
  }

  @Test
  public void setAndGetTaskExecutor() {
    simpleRqueueListenerContainerFactory.setTaskExecutor(taskExecutor);
    assertNotNull(simpleRqueueListenerContainerFactory.getTaskExecutor());
  }

  @Test
  public void setAutoStartup() {
    simpleRqueueListenerContainerFactory.setAutoStartup(false);
    assertFalse(simpleRqueueListenerContainerFactory.getAutoStartup());
  }

  @Test
  public void getAutoStartup() {
    assertTrue(simpleRqueueListenerContainerFactory.getAutoStartup());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setRqueueMessageHandler() {
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(null);
  }

  @Test
  public void getRqueueMessageHandler() {
    assertNull(simpleRqueueListenerContainerFactory.getRqueueMessageHandler());
  }

  @Test
  public void getBackOffTime() {
    assertNull(simpleRqueueListenerContainerFactory.getBackOffTime());
  }

  @Test
  public void setBackOffTime() {
    Long backOffTime = 1000L;
    simpleRqueueListenerContainerFactory.setBackOffTime(backOffTime);
    assertEquals(backOffTime, simpleRqueueListenerContainerFactory.getBackOffTime());
  }

  @Test
  public void setMaxNumWorkers() {
    Integer maxWorkers = 10;
    simpleRqueueListenerContainerFactory.setMaxNumWorkers(maxWorkers);
    assertEquals(maxWorkers, simpleRqueueListenerContainerFactory.getMaxNumWorkers());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setMessageConverters() {
    simpleRqueueListenerContainerFactory.setMessageConverters(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void setMessageConverters1() {
    simpleRqueueListenerContainerFactory.setMessageConverters(new ArrayList<>());
  }

  @Test
  public void getMessageConverters() {
    assertNull(simpleRqueueListenerContainerFactory.getMessageConverters());
  }

  @Test
  public void getMessageConverters2() {
    simpleRqueueListenerContainerFactory.setMessageConverters(
        Collections.singletonList(new GenericMessageConverter()));
    assertEquals(1, simpleRqueueListenerContainerFactory.getMessageConverters().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setRedisConnectionFactory() {
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(null);
  }

  @Test
  public void getRedisConnectionFactory() {
    assertNull(simpleRqueueListenerContainerFactory.getRedisConnectionFactory());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setRqueueMessageTemplate() {
    simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(null);
  }

  @Test
  public void getRqueueMessageTemplate() {
    assertNull(simpleRqueueListenerContainerFactory.getRqueueMessageTemplate());
  }

  @Test(expected = IllegalArgumentException.class)
  public void createMessageListenerContainer0() {
    simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Test(expected = IllegalArgumentException.class)
  public void createMessageListenerContainer1() {
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler());
    simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Test(expected = IllegalArgumentException.class)
  public void createMessageListenerContainer2() {
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.createMessageListenerContainer();
  }

  @Test
  public void createMessageListenerContainer3() {
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler());
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertNotNull(container.getRqueueMessageHandler());
    assertTrue(container.isAutoStartup());
    assertNotNull(simpleRqueueListenerContainerFactory.getRqueueMessageTemplate());
  }

  @Test
  public void deadLetterMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setDeadLetterQueueMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor,
        simpleRqueueListenerContainerFactory.getDeadLetterQueueMessageProcessor());
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler());
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(messageProcessor, container.getDeadLetterQueueMessageProcessor());
  }

  @Test(expected = IllegalArgumentException.class)
  public void discardMessageProcessorNull() {
    simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(null);
  }

  @Test
  public void discardMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor, simpleRqueueListenerContainerFactory.getDiscardMessageProcessor());
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler());
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(messageProcessor, container.getDiscardMessageProcessor());
  }
}
