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

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import io.lettuce.core.RedisCommandExecutionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageListenerContainerTest {
  private static final String slowQueue = "slow-queue";
  private static final String fastQueue = "fast-queue";
  private static final String slowProcessingQueue = "rqueue-processing::" + slowQueue;
  private static final String slowProcessingChannel = "rqueue-processing-channel::" + slowQueue;
  private static final String fastProcessingQueue = "rqueue-processing::" + fastQueue;
  private static final String fastProcessingQueueChannel =
      "rqueue-processing-channel::" + fastQueue;
  private RqueueMessageListenerContainer container =
      new RqueueMessageListenerContainer(
          mock(RqueueMessageHandler.class), mock(RqueueMessageTemplate.class));

  @Before
  public void init() throws IllegalAccessException {
    FieldUtils.writeField(
        container, "rqueueMessageMetadataService", mock(RqueueMessageMetadataService.class), true);
  }

  @Test
  public void testPollingInterval() {
    container.setPollingInterval(100L);
    assertEquals(100L, container.getPollingInterval());
  }

  @Test
  public void setMaxWorkerWaitTime() {
    container.setMaxWorkerWaitTime(20000L);
    assertEquals(20000L, container.getMaxWorkerWaitTime());
  }

  @Test
  public void setBeanName() {
    container.setBeanName("TestBean");
    assertEquals("TestBean", container.getBeanName());
  }

  @Test
  public void setMaxNumWorkers() {
    container.setMaxNumWorkers(1000);
    assertEquals(Integer.valueOf(1000), container.getMaxNumWorkers());
  }

  @Test
  public void setBackOffTime() {
    container.setBackOffTime(1000L);
    assertEquals(1000L, container.getBackOffTime());
  }

  @Test
  public void setAutoStartup() {
    container.setAutoStartup(false);
    assertFalse(container.isAutoStartup());
    container.setAutoStartup(true);
    assertTrue(container.isAutoStartup());
  }

  @Test
  public void setTaskExecutor() {
    assertNull(container.getTaskExecutor());
    ThreadPoolTaskExecutor asyncTaskExecutor = new ThreadPoolTaskExecutor();
    asyncTaskExecutor.setThreadNamePrefix("testExecutor");
    container.setTaskExecutor(asyncTaskExecutor);
    assertEquals(
        "testExecutor",
        ((ThreadPoolTaskExecutor) container.getTaskExecutor()).getThreadNamePrefix());
  }

  @Test
  public void testPhaseSetting() {
    assertEquals(Integer.MAX_VALUE, container.getPhase());
    container.setPhase(100);
    assertEquals(100, container.getPhase());
  }

  @Test
  public void checkDoStartMethodIsCalledAndIsRunningSet() throws Exception {
    StubMessageSchedulerListenerContainer container = new StubMessageSchedulerListenerContainer();
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    container.afterPropertiesSet();
    container.start();
    assertTrue(container.isRunning());
    assertTrue(container.isDoStartMethodIsCalled());
    assertFalse(container.isDestroyMethodIsCalled());
    assertFalse(container.isDoStopMethodIsCalled());
  }

  @Test
  public void checkDoStopMethodIsCalled() throws Exception {
    StubMessageSchedulerListenerContainer container = new StubMessageSchedulerListenerContainer();
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    container.afterPropertiesSet();
    container.start();
    container.stop();
    assertTrue(container.isDoStopMethodIsCalled());
  }

  @Test
  public void checkDoDestroyMethodIsCalled() throws Exception {
    StubMessageSchedulerListenerContainer container = new StubMessageSchedulerListenerContainer();
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    container.afterPropertiesSet();
    container.start();
    container.stop();
    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
  }

  @Test
  public void checkDoStopMethodIsCalledWithRunnable() throws Exception {
    StubMessageSchedulerListenerContainer container = new StubMessageSchedulerListenerContainer();
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    CountDownLatch count = new CountDownLatch(1);
    container.afterPropertiesSet();
    container.start();
    container.stop(count::countDown);

    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
    assertEquals(0, count.getCount());
  }

  @Test
  public void testMessagesAreGettingFetchedFromRedis() throws Exception {
    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    RqueueMessageListenerContainer container =
        createContainer(messageHandler, rqueueMessageTemplate);
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    AtomicInteger slowQueueCounter = new AtomicInteger(0);
    doAnswer(
            invocation -> {
              fastQueueCounter.incrementAndGet();
              return null;
            })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, 900000L);

    doAnswer(
            invocation -> {
              slowQueueCounter.incrementAndGet();
              return null;
            })
        .when(rqueueMessageTemplate)
        .pop(slowQueue, slowProcessingQueue, slowProcessingChannel, 900000L);
    container.afterPropertiesSet();
    container.start();
    waitFor(() -> fastQueueCounter.get() > 1, "fastQueue message call");
    waitFor(() -> slowQueueCounter.get() > 1, "slowQueue message call");
    container.stop();
    container.doDestroy();
  }

  private RqueueMessageListenerContainer createContainer(
      RqueueMessageHandler messageHandler, RqueueMessageTemplate rqueueMessageTemplate)
      throws IllegalAccessException {
    RqueueMessageListenerContainer container =
        new RqueueMessageListenerContainer(messageHandler, rqueueMessageTemplate);
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    FieldUtils.writeField(
        container, "rqueueMessageMetadataService", mock(RqueueMessageMetadataService.class), true);
    RqueueConfig rqueueConfig = new RqueueConfig(null, true, 1);
    FieldUtils.writeField(container, "rqueueConfig", rqueueConfig, true);
    return container;
  }

  @Test
  public void testMessageFetcherRetryWorking() throws Exception {
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    String fastQueueMessage = "This is fast queue";
    RqueueMessage message = new RqueueMessage(fastQueue, fastQueueMessage, null, null);

    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);

    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("rqueueConfig", RqueueConfig.class);
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    applicationContext.registerSingleton(
        "rqueueMessageMetadataService", RqueueMessageMetadataService.class);
    applicationContext.registerSingleton(
        "applicationEventPublisher", ApplicationEventPublisher.class);

    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    RqueueMessageListenerContainer container =
        createContainer(messageHandler, rqueueMessageTemplate);

    doAnswer(
            invocation -> {
              if (fastQueueCounter.get() < 2) {
                if (fastQueueCounter.incrementAndGet() == 1) {
                  throw new RedisCommandExecutionException("Some error occurred");
                }
                return message;
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, 900000L);
    FastMessageListener fastMessageListener =
        applicationContext.getBean("fastMessageListener", FastMessageListener.class);
    container.afterPropertiesSet();
    container.start();
    waitFor(() -> fastQueueCounter.get() == 2, "fastQueue message fetch");
    waitFor(
        () -> fastQueueMessage.equals(fastMessageListener.getLastMessage()),
        "message to be consumed");
    container.stop();
    container.doDestroy();
  }

  @Test
  public void testMessageHandlersAreInvoked() throws Exception {
    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    RqueueMessageListenerContainer container =
        createContainer(messageHandler, rqueueMessageTemplate);
    FastMessageListener fastMessageListener =
        applicationContext.getBean("fastMessageListener", FastMessageListener.class);
    SlowMessageListener slowMessageListener =
        applicationContext.getBean("slowMessageListener", SlowMessageListener.class);
    AtomicInteger slowQueueCounter = new AtomicInteger(0);
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    String fastQueueMessage = "This is fast queue";
    String slowQueueMessage = "This is slow queue";
    doAnswer(
            invocation -> {
              if (slowQueueCounter.get() == 0) {
                slowQueueCounter.incrementAndGet();
                return new RqueueMessage(slowQueue, slowQueueMessage, null, null);
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .pop(slowQueue, slowProcessingQueue, slowProcessingChannel, 900000L);

    doAnswer(
            invocation -> {
              if (fastQueueCounter.get() == 0) {
                fastQueueCounter.incrementAndGet();
                return new RqueueMessage(fastQueue, fastQueueMessage, null, null);
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, 900000L);
    container.afterPropertiesSet();
    container.start();
    waitFor(() -> slowQueueCounter.get() == 1, "slowQueue message fetch");
    waitFor(() -> fastQueueCounter.get() == 1, "fastQueue message fetch");
    waitFor(
        () -> fastQueueMessage.equals(fastMessageListener.getLastMessage()),
        "FastQueue message consumer call");
    waitFor(
        () -> slowQueueMessage.equals(slowMessageListener.getLastMessage()),
        "SlowQueue message consumer call");
    container.stop();
    container.doDestroy();
  }

  @Test
  public void internalTasksAreNotSharedWithTaskExecutor() throws Exception {
    @Getter
    class TestTaskExecutor extends ThreadPoolTaskExecutor {

      private static final long serialVersionUID = 8310240227553949352L;
      private int submittedTaskCount = 0;

      @Override
      public Future<?> submit(Runnable task) {
        submittedTaskCount += 1;
        return super.submit(task);
      }
    }

    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);

    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
    RqueueMessageListenerContainer container =
        createContainer(messageHandler, mock(RqueueMessageTemplate.class));
    TestTaskExecutor taskExecutor = new TestTaskExecutor();
    container.setTaskExecutor(taskExecutor);
    container.afterPropertiesSet();
    container.start();
    assertEquals(0, taskExecutor.getSubmittedTaskCount());
    container.stop();
    container.doDestroy();
  }

  @Getter
  private static class StubMessageSchedulerListenerContainer
      extends RqueueMessageListenerContainer {
    private boolean destroyMethodIsCalled = false;
    private boolean doStartMethodIsCalled = false;
    private boolean doStopMethodIsCalled = false;

    StubMessageSchedulerListenerContainer() {
      super(mock(RqueueMessageHandler.class), mock(RqueueMessageTemplate.class));
    }

    @Override
    protected void doStart() {
      doStartMethodIsCalled = true;
    }

    @Override
    protected void doStop() {
      doStopMethodIsCalled = true;
    }

    @Override
    protected void doDestroy() {
      destroyMethodIsCalled = true;
    }
  }

  @Getter
  private static class SlowMessageListener {
    private String lastMessage;

    @RqueueListener(value = slowQueue, delayedQueue = "true")
    public void onMessage(String message) {
      lastMessage = message;
    }
  }

  @Getter
  private static class FastMessageListener {
    private String lastMessage;

    @RqueueListener(fastQueue)
    public void onMessage(String message) {
      lastMessage = message;
    }
  }
}
