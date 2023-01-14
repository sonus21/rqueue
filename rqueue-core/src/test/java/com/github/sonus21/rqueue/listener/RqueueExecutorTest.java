/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueExecutorTest extends TestBase {

  private final QueueThreadPool queueThreadPool = new QueueThreadPool(null, true, 100);
  private final RqueueWebConfig rqueueWebConfig = new RqueueWebConfig();
  private final TestMessageProcessor deadLetterProcessor = new TestMessageProcessor();
  private final TestMessageProcessor discardProcessor = new TestMessageProcessor();
  private final TestMessageProcessor preProcessMessageProcessor = new TestMessageProcessor();
  private final TaskExecutionBackOff taskBackOff = new FixedTaskExecutionBackOff();
  private final String queueName = "test-queue";
  private final Object payload = "test message";
  @Mock
  private RqueueLockManager rqueueLockManager;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueJobDao rqueueJobDao;

  @Mock
  private RqueueMessageHandler messageHandler;
  @Mock
  private QueueStateMgr queueStateMgr;
  @Mock
  private RqueueBeanProvider rqueueBeanProvider;
  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock
  private RedisTemplate<String, RqueueMessage> redisTemplate;
  @Mock
  private ApplicationEventPublisher applicationEventPublisher;
  @Mock
  private RqueueMessageTemplate messageTemplate;

  private RqueueMessage rqueueMessage = new RqueueMessage();
  private PostProcessingHandler postProcessingHandler;
  private MessageMetadata defaultMessageMetadata;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    MessageConverter messageConverter = new GenericMessageConverter();
    rqueueMessage =
        RqueueMessageUtils.buildMessage(
            messageConverter,
            queueName,
            null,
            payload,
            null,
            null,
            RqueueMessageHeaders.emptyMessageHeaders());
    defaultMessageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    MessageProcessorHandler messageProcessorHandler =
        new MessageProcessorHandler(null, deadLetterProcessor, discardProcessor, null);
    postProcessingHandler =
        new PostProcessingHandler(
            rqueueWebConfig,
            applicationEventPublisher,
            messageTemplate,
            taskBackOff,
            messageProcessorHandler,
            rqueueSystemConfigDao);
    doReturn(rqueueMessageMetadataService)
        .when(rqueueBeanProvider)
        .getRqueueMessageMetadataService();
    doReturn(rqueueLockManager).when(rqueueBeanProvider).getRqueueLockManager();
    doReturn(true).when(queueStateMgr).isQueueActive(anyString());
    doReturn(messageHandler).when(rqueueBeanProvider).getRqueueMessageHandler();
    doReturn(messageConverter).when(messageHandler).getMessageConverter();
    doReturn(rqueueJobDao).when(rqueueBeanProvider).getRqueueJobDao();
    doReturn(rqueueConfig).when(rqueueBeanProvider).getRqueueConfig();
  }

  @Test
  void callDiscardProcessor() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(preProcessMessageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    doReturn(3).when(rqueueConfig).getRetryPerPoll();
    doThrow(new MessagingException("Failing for some reason."))
        .when(messageHandler)
        .handleMessage(any());
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();
    assertEquals(1, discardProcessor.getCount());
  }

  @Test
  void callDeadLetterProcessor() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(redisTemplate).when(messageTemplate).getTemplate();
    doReturn(preProcessMessageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    doThrow(new MessagingException("Failing for some reason."))
        .when(messageHandler)
        .handleMessage(any());
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName, "test-dlq");
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doReturn(Collections.emptyList()).when(redisTemplate)
        .executePipelined(any(RedisCallback.class));
    doReturn(3).when(rqueueConfig).getRetryPerPoll();
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();
    assertEquals(1, deadLetterProcessor.getCount());
  }

  @Test
  void messageIsParkedForRetry() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(preProcessMessageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();
    verify(messageTemplate, times(1))
        .moveMessageWithDelay(
            eq(queueDetail.getProcessingQueueName()),
            eq(queueDetail.getScheduledQueueName()),
            eq(rqueueMessage),
            any(),
            eq(5000L));
  }

  @Test
  void messageIsNotExecutedWhenDeletedManually() {
    doReturn(preProcessMessageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    messageMetadata.setDeleted(true);
    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(eq(rqueueMessage));
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();
    verify(messageHandler, times(0)).handleMessage(any());
  }

  @Test
  void messageIsDeletedWhileExecuting() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    AtomicInteger atomicInteger = new AtomicInteger(0);
    doReturn(preProcessMessageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    doReturn(2).when(rqueueConfig).getRetryPerPoll();
    doAnswer(
        invocation -> {
          if (atomicInteger.get() < 2) {
            atomicInteger.incrementAndGet();
            return messageMetadata;
          }
          messageMetadata.setDeleted(true);
          return messageMetadata;
        })
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(rqueueMessage);
    doReturn(messageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();
    verify(messageHandler, times(1)).handleMessage(any());
  }

  @Test
  void handleIgnoredMessage() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    MessageProcessor messageProcessor = job -> false;
    doReturn(messageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();
    verify(messageHandler, times(0)).handleMessage(any());
    verify(messageTemplate, times(1))
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
  }

  @Test
  void handlePeriodicMessage() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    long period = 10000L;
    RqueueMessage periodicMessage =
        rqueueMessage.toBuilder().period(period).processAt(System.currentTimeMillis()).build();
    RqueueMessage newMessage =
        periodicMessage.toBuilder().processAt(periodicMessage.nextProcessAt()).build();
    String messageKey =
        "__rq::queue::"
            + queueName
            + Constants.REDIS_KEY_SEPARATOR
            + periodicMessage.getId()
            + Constants.REDIS_KEY_SEPARATOR
            + "sch"
            + Constants.REDIS_KEY_SEPARATOR
            + newMessage.getProcessAt();
    doReturn(1).when(rqueueConfig).getRetryPerPoll();
    doReturn(preProcessMessageProcessor).when(rqueueBeanProvider).getPreExecutionMessageProcessor();
    doReturn(messageTemplate).when(rqueueBeanProvider).getRqueueMessageTemplate();
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        null,
        postProcessingHandler,
        periodicMessage,
        queueDetail,
        queueThreadPool)
        .run();
    verify(messageTemplate, times(1))
        .scheduleMessage(
            eq(queueDetail.getScheduledQueueName()), eq(messageKey), eq(newMessage), any());
    verify(messageHandler, times(1)).handleMessage(any());
  }

  private static class TestMessageProcessor implements MessageProcessor {

    private int count;

    @Override
    public boolean process(Job job) {
      count += 1;
      return true;
    }

    public void clear() {
      this.count = 0;
    }

    public int getCount() {
      return count;
    }
  }
}
