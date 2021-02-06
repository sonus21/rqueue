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

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import io.lettuce.core.RedisCommandExecutionException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.LinkedMultiValueMap;

@CoreUnitTest
class RqueueMessageListenerContainerTest extends TestBase {
  private static final String slowQueue = "slow-queue";
  private static final String fastQueue = "fast-queue";
  private static final String slowProcessingQueue = "rqueue-processing::" + slowQueue;
  private static final String slowProcessingChannel = "rqueue-processing-channel::" + slowQueue;
  private static final String fastProcessingQueue = "rqueue-processing::" + fastQueue;
  private static final String fastProcessingQueueChannel =
      "rqueue-processing-channel::" + fastQueue;
  private final RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
  private final RqueueMessageListenerContainer container =
      new RqueueMessageListenerContainer(rqueueMessageHandler, mock(RqueueMessageTemplate.class));
  private final RedisConnectionFactory redisConnectionFactory = mock(RedisConnectionFactory.class);
  private final RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, true, 1);

  @BeforeEach
  public void init() throws IllegalAccessException {
    FieldUtils.writeField(
        container, "rqueueMessageMetadataService", mock(RqueueMessageMetadataService.class), true);
  }

  @Test
  void pollingInterval() {
    container.setPollingInterval(100L);
    assertEquals(100L, container.getPollingInterval());
  }

  @Test
  void setMaxWorkerWaitTime() {
    container.setMaxWorkerWaitTime(20000L);
    assertEquals(20000L, container.getMaxWorkerWaitTime());
  }

  @Test
  void setBeanName() {
    container.setBeanName("TestBean");
    assertEquals("TestBean", container.getBeanName());
  }

  @Test
  void setMaxNumWorkers() {
    container.setMaxNumWorkers(1000);
    assertEquals(Integer.valueOf(1000), container.getMaxNumWorkers());
  }

  @Test
  void setBackOffTime() {
    container.setBackOffTime(1000L);
    assertEquals(1000L, container.getBackOffTime());
  }

  @Test
  void setAutoStartup() {
    container.setAutoStartup(false);
    assertFalse(container.isAutoStartup());
    container.setAutoStartup(true);
    assertTrue(container.isAutoStartup());
  }

  @Test
  void setTaskExecutor() {
    assertNull(container.getTaskExecutor());
    ThreadPoolTaskExecutor asyncTaskExecutor = new ThreadPoolTaskExecutor();
    asyncTaskExecutor.setThreadNamePrefix("testExecutor");
    container.setTaskExecutor(asyncTaskExecutor);
    assertEquals(
        "testExecutor",
        ((ThreadPoolTaskExecutor) container.getTaskExecutor()).getThreadNamePrefix());
  }

  @Test
  void phaseSetting() {
    assertEquals(Integer.MAX_VALUE, container.getPhase());
    container.setPhase(100);
    assertEquals(100, container.getPhase());
  }

  @Test
  void checkDoStartMethodIsCalledAndIsRunningSet() throws Exception {
    RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    FieldUtils.writeField(container, "rqueueConfig", rqueueConfig, true);
    container.afterPropertiesSet();
    container.start();
    assertTrue(container.isRunning());
    assertTrue(container.isDoStartMethodIsCalled());
    assertFalse(container.isDestroyMethodIsCalled());
    assertFalse(container.isDoStopMethodIsCalled());
  }

  @Test
  void checkDoStopMethodIsCalled() throws Exception {
    RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    FieldUtils.writeField(container, "rqueueConfig", rqueueConfig, true);
    container.afterPropertiesSet();
    container.start();
    container.stop();
    assertTrue(container.isDoStopMethodIsCalled());
  }

  @Test
  void checkDoDestroyMethodIsCalled() throws Exception {
    RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    FieldUtils.writeField(container, "rqueueConfig", rqueueConfig, true);
    container.afterPropertiesSet();
    container.start();
    container.stop();
    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
  }

  @Test
  void checkDoStopMethodIsCalledWithRunnable() throws Exception {
    RqueueMessageHandler rqueueMessageHandler = mock(RqueueMessageHandler.class);
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    CountDownLatch count = new CountDownLatch(1);
    FieldUtils.writeField(container, "rqueueConfig", rqueueConfig, true);
    container.afterPropertiesSet();
    container.start();
    container.stop(count::countDown);

    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
    assertEquals(0, count.getCount());
  }

  @Test
  void messagesAreGettingFetchedFromRedis() throws Exception {
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
        createContainer(
            rqueueConfig,
            messageHandler,
            rqueueMessageTemplate,
            mock(RqueueMessageMetadataService.class));
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
      RqueueConfig rqueueConfig,
      RqueueMessageHandler messageHandler,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueMessageMetadataService rqueueMessageMetadataService)
      throws IllegalAccessException {
    RqueueMessageListenerContainer container =
        new RqueueMessageListenerContainer(messageHandler, rqueueMessageTemplate);
    FieldUtils.writeField(
        container, "applicationEventPublisher", mock(ApplicationEventPublisher.class), true);
    FieldUtils.writeField(
        container, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
    FieldUtils.writeField(container, "rqueueConfig", rqueueConfig, true);
    FieldUtils.writeField(container, "rqueueJobDao", mock(RqueueJobDao.class), true);
    FieldUtils.writeField(container, "rqueueStringDao", mock(RqueueStringDao.class), true);
    return container;
  }

  @Test
  void messageFetcherRetryWorking() throws Exception {
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    String fastQueueMessage = "This is fast queue";
    RqueueMessage message =
        RqueueMessage.builder()
            .id(UUID.randomUUID().toString())
            .queueName(fastQueue)
            .message(fastQueueMessage)
            .processAt(System.currentTimeMillis())
            .queuedTime(System.nanoTime())
            .build();
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
    Map<String, MessageMetadata> messageMetadataMap = new ConcurrentHashMap<>();
    RqueueMessageMetadataService messageMetadataService = mock(RqueueMessageMetadataService.class);
    doAnswer(
            i -> {
              RqueueMessage rqueueMessage = i.getArgument(0);
              MessageMetadata messageMetadata =
                  new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
              messageMetadataMap.put(messageMetadata.getId(), messageMetadata);
              return messageMetadata;
            })
        .when(messageMetadataService)
        .getOrCreateMessageMetadata(any());
    RqueueMessageListenerContainer container =
        createContainer(
            rqueueConfig, messageHandler, rqueueMessageTemplate, messageMetadataService);

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
  void messageHandlersAreInvoked() throws Exception {
    RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageMetadataService messageMetadataService = mock(RqueueMessageMetadataService.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
    RqueueMessageListenerContainer container =
        createContainer(
            rqueueConfig, messageHandler, rqueueMessageTemplate, messageMetadataService);
    FastMessageListener fastMessageListener =
        applicationContext.getBean("fastMessageListener", FastMessageListener.class);
    SlowMessageListener slowMessageListener =
        applicationContext.getBean("slowMessageListener", SlowMessageListener.class);
    AtomicInteger slowQueueCounter = new AtomicInteger(0);
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    String fastQueueMessage = "This is fast queue";
    String slowQueueMessage = "This is slow queue";
    Map<String, MessageMetadata> messageMetadataMap = new ConcurrentHashMap<>();
    doAnswer(
            i -> {
              RqueueMessage rqueueMessage = i.getArgument(0);
              MessageMetadata messageMetadata =
                  new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
              messageMetadataMap.put(messageMetadata.getId(), messageMetadata);
              return messageMetadata;
            })
        .when(messageMetadataService)
        .getOrCreateMessageMetadata(any());
    doAnswer(
            invocation -> {
              if (slowQueueCounter.get() == 0) {
                slowQueueCounter.incrementAndGet();
                return RqueueMessage.builder()
                    .queueName(slowQueue)
                    .message(slowQueueMessage)
                    .processAt(System.currentTimeMillis())
                    .queuedTime(System.nanoTime())
                    .build();
              }
              return null;
            })
        .when(rqueueMessageTemplate)
        .pop(slowQueue, slowProcessingQueue, slowProcessingChannel, 900000L);

    doAnswer(
            invocation -> {
              if (fastQueueCounter.get() == 0) {
                fastQueueCounter.incrementAndGet();
                return RqueueMessage.builder()
                    .queueName(fastQueue)
                    .message(fastQueueMessage)
                    .processAt(System.currentTimeMillis())
                    .queuedTime(System.nanoTime())
                    .build();
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
  void internalTasksAreSubmittedToTaskExecutor() throws Exception {
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
        createContainer(
            rqueueConfig,
            messageHandler,
            mock(RqueueMessageTemplate.class),
            mock(RqueueMessageMetadataService.class));
    TestTaskExecutor taskExecutor = new TestTaskExecutor();
    taskExecutor.afterPropertiesSet();

    container.setTaskExecutor(taskExecutor);
    container.afterPropertiesSet();
    container.start();
    assertEquals(1, taskExecutor.getSubmittedTaskCount());
    container.stop();
    container.doDestroy();
  }

  @Getter
  private static class StubMessageSchedulerListenerContainer
      extends RqueueMessageListenerContainer {

    private boolean destroyMethodIsCalled = false;
    private boolean doStartMethodIsCalled = false;
    private boolean doStopMethodIsCalled = false;

    StubMessageSchedulerListenerContainer(RqueueMessageHandler rqueueMessageHandler) {
      super(rqueueMessageHandler, mock(RqueueMessageTemplate.class));
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

    @RqueueListener(value = slowQueue)
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
