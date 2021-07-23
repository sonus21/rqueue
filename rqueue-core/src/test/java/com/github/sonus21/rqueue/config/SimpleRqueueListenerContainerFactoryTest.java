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

package com.github.sonus21.rqueue.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.test.TestTaskExecutor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

@CoreUnitTest
class SimpleRqueueListenerContainerFactoryTest extends TestBase {
  private final AsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();
  private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;

  @BeforeEach
  void init() {
    simpleRqueueListenerContainerFactory = new SimpleRqueueListenerContainerFactory();
  }

  @Test
  void setTaskExecutor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setTaskExecutor(null));
  }

  @Test
  void setAndGetTaskExecutor() {
    simpleRqueueListenerContainerFactory.setTaskExecutor(taskExecutor);
    assertNotNull(simpleRqueueListenerContainerFactory.getTaskExecutor());
  }

  @Test
  void setAutoStartup() {
    simpleRqueueListenerContainerFactory.setAutoStartup(false);
    assertFalse(simpleRqueueListenerContainerFactory.getAutoStartup());
  }

  @Test
  void getAutoStartup() {
    assertTrue(simpleRqueueListenerContainerFactory.getAutoStartup());
  }

  @Test
  void setRqueueMessageHandler() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setRqueueMessageHandler(null));
  }

  @Test
  void getRqueueMessageHandler() {
    assertNotNull(simpleRqueueListenerContainerFactory.getRqueueMessageHandler());
  }

  @Test
  void getBackOffTime() {
    assertEquals(5000L, simpleRqueueListenerContainerFactory.getBackOffTime());
  }

  @Test
  void setBackOffTime() {
    long backOffTime = 1000L;
    simpleRqueueListenerContainerFactory.setBackOffTime(backOffTime);
    assertEquals(backOffTime, simpleRqueueListenerContainerFactory.getBackOffTime());
  }

  @Test
  void setMaxNumWorkers() {
    Integer maxWorkers = 10;
    simpleRqueueListenerContainerFactory.setMaxNumWorkers(maxWorkers);
    assertEquals(maxWorkers, simpleRqueueListenerContainerFactory.getMaxNumWorkers());
  }

  @Test
  void getMessageConverter() {
    assertNotNull(simpleRqueueListenerContainerFactory.getMessageConverter());
  }

  @Test
  void setRedisConnectionFactory() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setRedisConnectionFactory(null));
  }

  @Test
  void getRedisConnectionFactory() {
    assertNull(simpleRqueueListenerContainerFactory.getRedisConnectionFactory());
  }

  @Test
  void setRqueueMessageTemplate() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(null));
  }

  @Test
  void getRqueueMessageTemplate() {
    assertNull(simpleRqueueListenerContainerFactory.getRqueueMessageTemplate());
  }

  @Test
  void createMessageListenerContainer0() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.createMessageListenerContainer());
  }

  @Test
  void createMessageListenerContainer1() {
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(
        new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.createMessageListenerContainer());
  }

  @Test
  void createMessageListenerContainer3() {
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(
        new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertNotNull(container.getRqueueMessageHandler());
    assertTrue(container.isAutoStartup());
    assertNotNull(simpleRqueueListenerContainerFactory.getRqueueMessageTemplate());
  }

  @Test
  void deadLetterMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setDeadLetterQueueMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor,
        simpleRqueueListenerContainerFactory.getDeadLetterQueueMessageProcessor());
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(
        new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(messageProcessor, container.getDeadLetterQueueMessageProcessor());
  }

  @Test
  void discardMessageProcessorNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(null));
  }

  @Test
  void discardMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor, simpleRqueueListenerContainerFactory.getDiscardMessageProcessor());
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(
        new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(messageProcessor, container.getDiscardMessageProcessor());
  }

  @Test
  void getTaskExecutionBackOff() {
    assertNull(simpleRqueueListenerContainerFactory.getTaskExecutionBackOff());
  }

  @Test
  void setNullTaskExecutionBackOff() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(null));
  }

  @Test
  void setTaskExecutionBackOff() {
    FixedTaskExecutionBackOff backOff = new FixedTaskExecutionBackOff(10000L, 10);
    simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(backOff);
    assertEquals(backOff, simpleRqueueListenerContainerFactory.getTaskExecutionBackOff());
  }

  @Test
  void getPriorityMode() {
    assertEquals(PriorityMode.WEIGHTED, simpleRqueueListenerContainerFactory.getPriorityMode());
  }

  @Test
  void setPriorityMode() {
    simpleRqueueListenerContainerFactory.setPriorityMode(PriorityMode.STRICT);
    assertEquals(PriorityMode.STRICT, simpleRqueueListenerContainerFactory.getPriorityMode());
  }

  @Test
  void setPollingInterval() {
    simpleRqueueListenerContainerFactory.setPollingInterval(1000L);
    assertEquals(1000L, simpleRqueueListenerContainerFactory.getPollingInterval());
  }

  @Test
  void setNullPreExecutionMessageProcessor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(null));
  }

  @Test
  void setPreExecutionMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor.hashCode(),
        simpleRqueueListenerContainerFactory.getPreExecutionMessageProcessor().hashCode());
  }

  @Test
  void setNullPostExecutionMessageProcessor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(null));
  }

  @Test
  void setPostExecutionMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor.hashCode(),
        simpleRqueueListenerContainerFactory.getPostExecutionMessageProcessor().hashCode());
  }

  @Test
  void createContainer() {
    MessageProcessor pre = new MessageProcessor() {};
    MessageProcessor post = new MessageProcessor() {};
    MessageProcessor deadLetter = new MessageProcessor() {};
    MessageProcessor deletion = new MessageProcessor() {};
    MessageProcessor discardMessageProcessor = new MessageProcessor() {};
    TaskExecutionBackOff backOff = new FixedTaskExecutionBackOff(1000L, 4);
    AsyncTaskExecutor executor = new TestTaskExecutor(false);
    simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(post);
    simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(pre);
    simpleRqueueListenerContainerFactory.setDeadLetterQueueMessageProcessor(deadLetter);
    simpleRqueueListenerContainerFactory.setManualDeletionMessageProcessor(deletion);
    simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(discardMessageProcessor);
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(
        new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(backOff);
    simpleRqueueListenerContainerFactory.setTaskExecutor(executor);
    simpleRqueueListenerContainerFactory.setPriorityMode(PriorityMode.WEIGHTED);

    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(PriorityMode.WEIGHTED, container.getPriorityMode());
    assertEquals(backOff, container.getTaskExecutionBackOff());
    assertEquals(executor, container.getTaskExecutor());
    assertEquals(pre, container.getPreExecutionMessageProcessor());
    assertEquals(post, container.getPostExecutionMessageProcessor());
    assertEquals(deadLetter, container.getDeadLetterQueueMessageProcessor());
    assertEquals(deletion, container.getManualDeletionMessageProcessor());
    assertEquals(discardMessageProcessor, container.getDiscardMessageProcessor());
  }
}
