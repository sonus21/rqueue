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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

@ExtendWith(MockitoExtension.class)
public class SimpleRqueueListenerContainerFactoryTest {
  private SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory;
  private AsyncTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

  @BeforeEach
  public void init() {
    simpleRqueueListenerContainerFactory = new SimpleRqueueListenerContainerFactory();
  }

  @Test
  public void setTaskExecutor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setTaskExecutor(null));
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

  @Test
  public void setRqueueMessageHandler() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setRqueueMessageHandler(null));
  }

  @Test
  public void getRqueueMessageHandler() {
    assertNotNull(simpleRqueueListenerContainerFactory.getRqueueMessageHandler());
  }

  @Test
  public void getBackOffTime() {
    assertEquals(5000L, simpleRqueueListenerContainerFactory.getBackOffTime());
  }

  @Test
  public void setBackOffTime() {
    long backOffTime = 1000L;
    simpleRqueueListenerContainerFactory.setBackOffTime(backOffTime);
    assertEquals(backOffTime, simpleRqueueListenerContainerFactory.getBackOffTime());
  }

  @Test
  public void setMaxNumWorkers() {
    Integer maxWorkers = 10;
    simpleRqueueListenerContainerFactory.setMaxNumWorkers(maxWorkers);
    assertEquals(maxWorkers, simpleRqueueListenerContainerFactory.getMaxNumWorkers());
  }

  @Test
  public void setMessageConverters() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setMessageConverters(null));
  }

  @Test
  public void setMessageConverters1() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setMessageConverters(new ArrayList<>()));
  }

  @Test
  public void getMessageConverter() {
    assertNotNull(simpleRqueueListenerContainerFactory.getMessageConverter());
  }

  @Test
  public void setRedisConnectionFactory() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setRedisConnectionFactory(null));
  }

  @Test
  public void getRedisConnectionFactory() {
    assertNull(simpleRqueueListenerContainerFactory.getRedisConnectionFactory());
  }

  @Test
  public void setRqueueMessageTemplate() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setRqueueMessageTemplate(null));
  }

  @Test
  public void getRqueueMessageTemplate() {
    assertNull(simpleRqueueListenerContainerFactory.getRqueueMessageTemplate());
  }

  @Test
  public void createMessageListenerContainer0() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.createMessageListenerContainer());
  }

  @Test
  public void createMessageListenerContainer1() {
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.createMessageListenerContainer());
  }

  @Test
  public void createMessageListenerContainer3() {
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
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
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(messageProcessor, container.getDeadLetterQueueMessageProcessor());
  }

  @Test
  public void discardMessageProcessorNull() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(null));
  }

  @Test
  public void discardMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor, simpleRqueueListenerContainerFactory.getDiscardMessageProcessor());
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
    RqueueMessageListenerContainer container =
        simpleRqueueListenerContainerFactory.createMessageListenerContainer();
    assertNotNull(container);
    assertEquals(messageProcessor, container.getDiscardMessageProcessor());
  }

  @Test
  public void getTaskExecutionBackOff() {
    assertNull(simpleRqueueListenerContainerFactory.getTaskExecutionBackOff());
  }

  @Test
  public void setNullTaskExecutionBackOff() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(null));
  }

  @Test
  public void setTaskExecutionBackOff() {
    FixedTaskExecutionBackOff backOff = new FixedTaskExecutionBackOff(10000L, 10);
    simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(backOff);
    assertEquals(backOff, simpleRqueueListenerContainerFactory.getTaskExecutionBackOff());
  }

  @Test
  public void getPriorityMode() {
    assertNull(simpleRqueueListenerContainerFactory.getPriorityMode());
  }

  @Test
  public void setPriorityMode() {
    simpleRqueueListenerContainerFactory.setPriorityMode(PriorityMode.STRICT);
    assertEquals(PriorityMode.STRICT, simpleRqueueListenerContainerFactory.getPriorityMode());
  }

  @Test
  public void setPollingInterval() {
    simpleRqueueListenerContainerFactory.setPollingInterval(1000L);
    assertEquals(1000L, simpleRqueueListenerContainerFactory.getPollingInterval());
  }

  @Test
  public void setNullPreExecutionMessageProcessor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(null));
  }

  @Test
  public void setPreExecutionMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor.hashCode(),
        simpleRqueueListenerContainerFactory.getPreExecutionMessageProcessor().hashCode());
  }

  @Test
  public void setNullPostExecutionMessageProcessor() {
    assertThrows(
        IllegalArgumentException.class,
        () -> simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(null));
  }

  @Test
  public void setPostExecutionMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor.hashCode(),
        simpleRqueueListenerContainerFactory.getPostExecutionMessageProcessor().hashCode());
  }

  @Test
  public void createContainer() {
    MessageProcessor pre = new MessageProcessor() {};
    MessageProcessor post = new MessageProcessor() {};
    MessageProcessor deadLetter = new MessageProcessor() {};
    MessageProcessor deletion = new MessageProcessor() {};
    MessageProcessor discardMessageProcessor = new MessageProcessor() {};
    TaskExecutionBackOff backOff = new FixedTaskExecutionBackOff(1000L, 4);
    AsyncTaskExecutor executor = new TestAsyncTaskExecutor();
    simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(post);
    simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(pre);
    simpleRqueueListenerContainerFactory.setDeadLetterQueueMessageProcessor(deadLetter);
    simpleRqueueListenerContainerFactory.setManualDeletionMessageProcessor(deletion);
    simpleRqueueListenerContainerFactory.setDiscardMessageProcessor(discardMessageProcessor);
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(new LettuceConnectionFactory());
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler(new DefaultRqueueMessageConverter()));
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

  private class TestAsyncTaskExecutor implements AsyncTaskExecutor {

    @Override
    public void execute(Runnable task, long startTimeout) {}

    @Override
    public Future<?> submit(Runnable task) {
      return null;
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
      return null;
    }

    @Override
    public void execute(Runnable task) {}
  }
}
