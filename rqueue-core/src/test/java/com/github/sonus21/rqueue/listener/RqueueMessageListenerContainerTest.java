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

package com.github.sonus21.rqueue.listener;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verifyNoInteractions;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.github.sonus21.test.TestTaskExecutor;
import io.lettuce.core.RedisCommandExecutionException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationListener;
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
  private static final long VISIBILITY_TIMEOUT = 900000L;
  @Mock
  private RqueueMessageHandler rqueueMessageHandler;
  @Mock
  private RedisConnectionFactory redisConnectionFactory;
  @Mock
  private ApplicationEventPublisher applicationEventPublisher;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueWebConfig rqueueWebConfig;
  @Mock
  private RqueueLockManager rqueueLockManager;
  private RqueueMessageListenerContainer container;
  private RqueueBeanProvider beanProvider;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, true, 1);
    beanProvider = new RqueueBeanProvider();
    beanProvider.setRqueueConfig(rqueueConfig);
    beanProvider.setRqueueMessageHandler(rqueueMessageHandler);
    beanProvider.setRqueueSystemConfigDao(rqueueSystemConfigDao);
    beanProvider.setApplicationEventPublisher(applicationEventPublisher);
    beanProvider.setRqueueMessageTemplate(rqueueMessageTemplate);
    beanProvider.setRqueueMessageMetadataService(rqueueMessageMetadataService);
    beanProvider.setRqueueWebConfig(rqueueWebConfig);
    beanProvider.setRqueueLockManager(rqueueLockManager);
    container = new TestListenerContainer(rqueueMessageHandler);
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
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    container.afterPropertiesSet();
    container.start();
    assertTrue(container.isRunning());
    assertTrue(container.isDoStartMethodIsCalled());
    assertFalse(container.isDestroyMethodIsCalled());
    assertFalse(container.isDoStopMethodIsCalled());
  }

  @Test
  void checkDoStopMethodIsCalled() throws Exception {
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    container.afterPropertiesSet();
    container.start();
    container.stop();
    assertTrue(container.isDoStopMethodIsCalled());
  }

  @Test
  void checkDoDestroyMethodIsCalled() throws Exception {
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    container.afterPropertiesSet();
    container.start();
    container.stop();
    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
  }

  @Test
  void checkDoStopMethodIsCalledWithRunnable() throws Exception {
    doReturn(new LinkedMultiValueMap<>()).when(rqueueMessageHandler).getHandlerMethodMap();
    StubMessageSchedulerListenerContainer container =
        new StubMessageSchedulerListenerContainer(rqueueMessageHandler);
    CountDownLatch count = new CountDownLatch(1);
    container.afterPropertiesSet();
    container.start();
    container.stop(count::countDown);

    container.destroy();
    assertTrue(container.isDestroyMethodIsCalled());
    assertEquals(0, count.getCount());
  }

  @Test
  void messagesAreGettingFetchedFromRedis() throws Exception {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);

    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
    beanProvider.setRqueueMessageHandler(rqueueMessageHandler);
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);

    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    AtomicInteger slowQueueCounter = new AtomicInteger(0);
    doAnswer(
        invocation -> {
          fastQueueCounter.incrementAndGet();
          return null;
        })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, VISIBILITY_TIMEOUT, 1);

    doAnswer(
        invocation -> {
          slowQueueCounter.incrementAndGet();
          return null;
        })
        .when(rqueueMessageTemplate)
        .pop(slowQueue, slowProcessingQueue, slowProcessingChannel, VISIBILITY_TIMEOUT, 1);
    container.afterPropertiesSet();
    container.start();
    waitFor(() -> fastQueueCounter.get() > 1, "fastQueue message call");
    waitFor(() -> slowQueueCounter.get() > 1, "slowQueue message call");
    container.stop();
    container.doDestroy();
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
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
    Map<String, MessageMetadata> messageMetadataMap = new ConcurrentHashMap<>();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doAnswer(i -> messageMetadataMap.get(i.getArgument(0))).when(rqueueMessageMetadataService)
        .get(any());
    doAnswer(
        i -> {
          RqueueMessage rqueueMessage = i.getArgument(0);
          MessageMetadata messageMetadata =
              new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
          messageMetadataMap.put(messageMetadata.getId(), messageMetadata);
          return messageMetadata;
        })
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());

    beanProvider.setRqueueMessageHandler(messageHandler);
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);

    doAnswer(
        invocation -> {
          if (fastQueueCounter.get() < 2) {
            if (fastQueueCounter.incrementAndGet() == 1) {
              throw new RedisCommandExecutionException("Some error occurred");
            }
            return Collections.singletonList(message);
          }
          return null;
        })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, VISIBILITY_TIMEOUT, 1);
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
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("fastMessageListener", FastMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
    beanProvider.setRqueueMessageHandler(messageHandler);
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);
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
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    doAnswer(i -> messageMetadataMap.get(i.getArgument(0))).when(rqueueMessageMetadataService)
        .get(any());
    doAnswer(
        invocation -> {
          if (slowQueueCounter.get() == 0) {
            slowQueueCounter.incrementAndGet();
            return Collections.singletonList(
                RqueueMessage.builder()
                    .queueName(slowQueue)
                    .message(slowQueueMessage)
                    .processAt(System.currentTimeMillis())
                    .queuedTime(System.nanoTime())
                    .id(UUID.randomUUID().toString())
                    .build());
          }
          return null;
        })
        .when(rqueueMessageTemplate)
        .pop(slowQueue, slowProcessingQueue, slowProcessingChannel, VISIBILITY_TIMEOUT, 1);

    doAnswer(
        invocation -> {
          if (fastQueueCounter.get() == 0) {
            fastQueueCounter.incrementAndGet();
            return Collections.singletonList(
                RqueueMessage.builder()
                    .queueName(fastQueue)
                    .message(fastQueueMessage)
                    .processAt(System.currentTimeMillis())
                    .queuedTime(System.nanoTime())
                    .id(UUID.randomUUID().toString())
                    .build());
          }
          return null;
        })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, VISIBILITY_TIMEOUT, 1);
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
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    beanProvider.setRqueueMessageHandler(messageHandler);
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);

    TestTaskExecutor taskExecutor = new TestTaskExecutor();
    taskExecutor.afterPropertiesSet();

    container.setTaskExecutor(taskExecutor);
    container.afterPropertiesSet();
    container.start();
    assertEquals(1, taskExecutor.getSubmittedTaskCount());
    container.stop();
    container.doDestroy();
  }

  @Test
  void pausedQueueShouldNotBePolled() throws Exception {
    BootstrapEventListener listener = new BootstrapEventListener();
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton("slowMessageListener", SlowMessageListener.class);
    applicationContext.registerSingleton("applicationEventPublisher", TestEventBroadcaster.class);

    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    TestEventBroadcaster eventBroadcaster =
        applicationContext.getBean("applicationEventPublisher", TestEventBroadcaster.class);
    eventBroadcaster.subscribe(listener);

    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();
    QueueConfig queueConfig = TestUtils.createQueueConfig(slowQueue);
    queueConfig.setPaused(true);
    doReturn(queueConfig).when(rqueueSystemConfigDao).getConfigByName(slowQueue);

    beanProvider.setApplicationEventPublisher(eventBroadcaster);
    beanProvider.setRqueueMessageHandler(messageHandler);
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);

    TestTaskExecutor taskExecutor = new TestTaskExecutor();
    taskExecutor.afterPropertiesSet();

    container.setTaskExecutor(taskExecutor);
    container.afterPropertiesSet();
    container.start();

    TimeoutUtils.waitFor(listener::isStartEventReceived, "start event");
    TimeoutUtils.sleep(Constants.ONE_MILLI);
    container.stop();
    TimeoutUtils.waitFor(listener::isStopEventReceived, "stop event");
    container.doDestroy();
    verifyNoInteractions(rqueueMessageTemplate);
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

  @Getter
  static class BootstrapEventListener implements ApplicationListener<RqueueBootstrapEvent> {

    private boolean startEventReceived;
    private boolean stopEventReceived;

    @Override
    public void onApplicationEvent(RqueueBootstrapEvent event) {
      if (event.isStart()) {
        startEventReceived = true;
      } else {
        stopEventReceived = true;
      }
    }
  }

  @SuppressWarnings("unchecked")
  static class TestEventBroadcaster implements ApplicationEventPublisher {

    List<ApplicationListener> listenerList = new LinkedList<>();

    void subscribe(ApplicationListener listener) {
      listenerList.add(listener);
    }

    @Override
    public void publishEvent(ApplicationEvent event) {
      for (ApplicationListener listener : listenerList) {
        listener.onApplicationEvent(event);
      }
    }

    @Override
    public void publishEvent(Object event) {
    }
  }

  private class TestListenerContainer extends RqueueMessageListenerContainer {

    TestListenerContainer(RqueueMessageHandler rqueueMessageHandler) {
      super(rqueueMessageHandler, rqueueMessageTemplate);
      this.rqueueBeanProvider = beanProvider;
    }
  }

  @Getter
  private class StubMessageSchedulerListenerContainer extends RqueueMessageListenerContainer {

    private boolean destroyMethodIsCalled = false;
    private boolean doStartMethodIsCalled = false;
    private boolean doStopMethodIsCalled = false;

    StubMessageSchedulerListenerContainer(RqueueMessageHandler rqueueMessageHandler) {
      super(rqueueMessageHandler, rqueueMessageTemplate);
      this.rqueueBeanProvider = beanProvider;
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
}
