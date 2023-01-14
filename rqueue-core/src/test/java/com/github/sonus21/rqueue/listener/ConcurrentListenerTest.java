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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.AtomicValueHolder;
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
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.support.StaticApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@CoreUnitTest
@Slf4j
class ConcurrentListenerTest extends TestBase {

  private static final String fastQueue = "fast-queue";
  private static final String fastProcessingQueue = "rqueue-processing::" + fastQueue;
  private static final String fastProcessingQueueChannel =
      "rqueue-processing-channel::" + fastQueue;
  private static final long executionTime = 50L;
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
  private RqueueLockManager rqueueLockManager;
  @Mock
  private RqueueWebConfig rqueueWebConfig;
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
    beanProvider.setRqueueLockManager(rqueueLockManager);
    beanProvider.setRqueueWebConfig(rqueueWebConfig);
  }

  @Test
  void validateConcurrency() throws Exception {
    StaticApplicationContext applicationContext = new StaticApplicationContext();
    applicationContext.registerSingleton("messageHandler", RqueueMessageHandler.class);
    applicationContext.registerSingleton(
        "fastMessageListener", ConcurrentListenerTest.FastMessageListener.class);

    RqueueMessageHandler messageHandler =
        applicationContext.getBean("messageHandler", RqueueMessageHandler.class);
    FastMessageListener fastMessageListener =
        applicationContext.getBean("fastMessageListener", FastMessageListener.class);
    messageHandler.setApplicationContext(applicationContext);
    messageHandler.afterPropertiesSet();

    beanProvider.setRqueueMessageHandler(messageHandler);
    RqueueMessageListenerContainer container = new TestListenerContainer(messageHandler);
    AtomicValueHolder<Long> lastCalledAt = new AtomicValueHolder<>();
    AtomicValueHolder<Long> firstCallAt = new AtomicValueHolder<>();
    AtomicInteger producerMessageCounter = new AtomicInteger(0);
    AtomicInteger pollCounter = new AtomicInteger(0);
    Map<String, MessageMetadata> messageMetadataMap = new HashMap<>();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doAnswer(
        i -> {
          RqueueMessage rqueueMessage = i.getArgument(0);
          MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage,
              MessageStatus.ENQUEUED);
          messageMetadataMap.put(messageMetadata.getId(), messageMetadata);
          return messageMetadata;
        })
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    doAnswer(
        i -> messageMetadataMap.get(i.getArgument(0)))
        .when(rqueueMessageMetadataService)
        .get(any());
    doAnswer(
        invocation -> {
          if (1 == pollCounter.incrementAndGet()) {
            firstCallAt.set(System.currentTimeMillis());
          }
          int count = invocation.getArgument(4);
          List<RqueueMessage> rqueueMessageList = new LinkedList<>();
          for (int i = 0; i < count; i++) {
            int id = producerMessageCounter.incrementAndGet();
            RqueueMessage rqueueMessage =
                RqueueMessage.builder()
                    .message("Message ::" + id + "::" + i)
                    .id(UUID.randomUUID().toString())
                    .queueName(fastQueue)
                    .processAt(System.currentTimeMillis())
                    .queuedTime(System.currentTimeMillis())
                    .build();
            rqueueMessageList.add(rqueueMessage);
          }
          lastCalledAt.set(System.currentTimeMillis());
          return rqueueMessageList;
        })
        .when(rqueueMessageTemplate)
        .pop(
            eq(fastQueue),
            eq(fastProcessingQueue),
            eq(fastProcessingQueueChannel),
            eq(900000L),
            anyInt());
    container.afterPropertiesSet();
    container.start();

    long end = System.currentTimeMillis() + TimeoutUtils.EXECUTION_TIME;
    long aboutToEnd = end - 3 * TimeoutUtils.SLEEP_TIME;
    AtomicInteger consumedMessageCounter = fastMessageListener.totalMessages;
    waitFor(
        () -> {
          int count = producerMessageCounter.get();
          int consumed = consumedMessageCounter.get();
          long now = System.currentTimeMillis();
          log.info("Now: {}, Produced Messages: {}, Consumed Messages: {}", now, count, consumed);
          return now >= aboutToEnd;
        },
        "fastQueue message call");
    container.stop();
    log.info("Consumed Messages {}", consumedMessageCounter.get());
    log.info("Produced Messages {}", producerMessageCounter.get());
    log.info("Time elapsed {}", lastCalledAt.get() - firstCallAt.get());
    container.doDestroy();
  }

  @Getter
  @Slf4j
  private static class FastMessageListener {

    private final AtomicInteger totalMessages = new AtomicInteger(0);
    private final AtomicValueHolder<Long> holder = new AtomicValueHolder<>();
    private final Random random = new Random(System.currentTimeMillis());

    @RqueueListener(value = fastQueue, concurrency = "20-40", batchSize = "20")
    public void onMessage(String message) {
      long start = System.currentTimeMillis();
      totalMessages.incrementAndGet();
      holder.set(System.currentTimeMillis());
      TimeoutUtils.sleep(executionTime);
      long end = System.currentTimeMillis();
      log.info("Message: {} Execution time {}", message, end - start);
    }
  }

  private class TestListenerContainer extends RqueueMessageListenerContainer {

    TestListenerContainer(RqueueMessageHandler rqueueMessageHandler) {
      super(rqueueMessageHandler, rqueueMessageTemplate);
      this.rqueueBeanProvider = beanProvider;
    }
  }
}
