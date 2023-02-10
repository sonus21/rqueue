/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainerTest.BootstrapEventListener;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainerTest.TestEventBroadcaster;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.PriorityMode;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.github.sonus21.test.TestTaskExecutor;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@CoreUnitTest
class PriorityGroupListenerTest extends TestBase {

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
  }

  @Test
  void priorityGroupListener() throws Exception {
    BootstrapEventListener listener = new BootstrapEventListener();
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageConverter", DefaultRqueueMessageConverter.class);
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton(
        "slowMessageListener", SlowMessageListenerWithPriority.class);
    applicationContext.registerSingleton(
        "fastMessageListener", FastMessageListenerWithPriority.class);
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
    QueueConfig queueConfig2 = TestUtils.createQueueConfig(fastQueue);
    doReturn(Arrays.asList(queueConfig, queueConfig2))
        .when(rqueueSystemConfigDao)
        .getConfigByNames(any());

    beanProvider.setApplicationEventPublisher(eventBroadcaster);
    beanProvider.setRqueueMessageHandler(messageHandler);
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);

    TestTaskExecutor taskExecutor = new TestTaskExecutor();
    taskExecutor.afterPropertiesSet();
    AtomicInteger fastQueueCounter = new AtomicInteger(0);
    doAnswer(
        invocation -> {
          fastQueueCounter.incrementAndGet();
          return Collections.singletonList(
              RqueueMessage.builder()
                  .queueName(fastQueue)
                  .message("fastQueueMessage")
                  .processAt(System.currentTimeMillis())
                  .queuedTime(System.nanoTime())
                  .id(UUID.randomUUID().toString())
                  .build());
        })
        .when(rqueueMessageTemplate)
        .pop(fastQueue, fastProcessingQueue, fastProcessingQueueChannel, VISIBILITY_TIMEOUT, 1);

    container.setPriorityMode(PriorityMode.STRICT);
    container.setTaskExecutor(taskExecutor);
    container.afterPropertiesSet();
    container.start();
    TimeoutUtils.waitFor(listener::isStartEventReceived, "start event");
    TimeoutUtils.waitFor(() -> fastQueueCounter.get() > 1, "fast queue message poll");

    container.stop();
    TimeoutUtils.waitFor(listener::isStopEventReceived, "stop event");
    container.doDestroy();
    verify(rqueueMessageTemplate, times(0))
        .pop(slowQueue, slowProcessingQueue, slowProcessingChannel, VISIBILITY_TIMEOUT, 1);
  }

  @Getter
  private static class SlowMessageListenerWithPriority {

    private String lastMessage;

    @RqueueListener(value = slowQueue, priority = "10", priorityGroup = "pg")
    public void onMessage(String message) {
      lastMessage = message;
    }
  }

  @Getter
  private static class FastMessageListenerWithPriority {

    private String lastMessage;

    @RqueueListener(value = fastQueue, priority = "100", priorityGroup = "pg")
    public void onMessage(String message) {
      lastMessage = message;
    }
  }

  private class TestListenerContainer extends RqueueMessageListenerContainer {

    TestListenerContainer(RqueueMessageHandler rqueueMessageHandler) {
      super(rqueueMessageHandler, rqueueMessageTemplate);
      this.rqueueBeanProvider = beanProvider;
    }
  }
}
