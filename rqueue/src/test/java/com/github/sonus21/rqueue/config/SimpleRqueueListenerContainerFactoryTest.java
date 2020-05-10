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
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
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

  @Test
  public void getTaskExecutionBackOff() {
    assertNull(simpleRqueueListenerContainerFactory.getTaskExecutionBackOff());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setNullTaskExecutionBackOff() {
    simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(null);
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

  @Test(expected = IllegalArgumentException.class)
  public void setNullPreExecutionMessageProcessor() {
    simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(null);
  }

  @Test
  public void setPreExecutionMessageProcessor() {
    MessageProcessor messageProcessor = new MessageProcessor() {};
    simpleRqueueListenerContainerFactory.setPreExecutionMessageProcessor(messageProcessor);
    assertEquals(
        messageProcessor.hashCode(),
        simpleRqueueListenerContainerFactory.getPreExecutionMessageProcessor().hashCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void setNullPostExecutionMessageProcessor() {
    simpleRqueueListenerContainerFactory.setPostExecutionMessageProcessor(null);
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
    simpleRqueueListenerContainerFactory.setRqueueMessageHandler(new RqueueMessageHandler());
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
