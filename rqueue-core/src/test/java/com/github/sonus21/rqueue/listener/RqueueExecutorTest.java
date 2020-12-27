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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.lang.ref.WeakReference;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
@SuppressWarnings("unchecked")
class RqueueExecutorTest extends TestBase {
  private RqueueMessageListenerContainer container = mock(RqueueMessageListenerContainer.class);
  private WeakReference<RqueueMessageListenerContainer> containerWeakReference =
      new WeakReference<>(container);
  private RqueueWebConfig rqueueWebConfig = new RqueueWebConfig();
  private RqueueConfig rqueueConfig = mock(RqueueConfig.class);
  private RqueueMessageMetadataService rqueueMessageMetadataService =
      mock(RqueueMessageMetadataService.class);
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueJobDao rqueueJobDao = mock(RqueueJobDao.class);
  private RqueueStringDao rqueueStringDao = mock(RqueueStringDao.class);
  private TestMessageProcessor deadLetterProcessor = new TestMessageProcessor();
  private TestMessageProcessor discardProcessor = new TestMessageProcessor();
  private TestMessageProcessor preProcessMessageProcessor = new TestMessageProcessor();
  private RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
  private RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
  private RqueueMessage rqueueMessage = new RqueueMessage();
  private Semaphore semaphore = new Semaphore(100);
  private TaskExecutionBackOff taskBackOff = new FixedTaskExecutionBackOff();
  private PostProcessingHandler postProcessingHandler;
  private ApplicationEventPublisher applicationEventPublisher =
      mock(ApplicationEventPublisher.class);
  private RqueueSystemConfigDao rqueueSystemConfigDao = mock(RqueueSystemConfigDao.class);
  private String queueName = "test-queue";
  private MessageMetadata defaultMessageMetadata;

  @BeforeEach
  public void init() throws IllegalAccessException {
    rqueueMessage.setMessage("test message");
    MessageProcessorHandler messageProcessorHandler =
        new MessageProcessorHandler(null, deadLetterProcessor, discardProcessor, null);
    postProcessingHandler =
        new PostProcessingHandler(
            rqueueConfig,
            rqueueWebConfig,
            applicationEventPublisher,
            messageTemplate,
            taskBackOff,
            messageProcessorHandler,
            rqueueSystemConfigDao);
    MessageConverter messageConverter = new GenericMessageConverter();
    rqueueMessage.setId(UUID.randomUUID().toString());
    doReturn(rqueueMessageMetadataService).when(container).rqueueMessageMetadataService();
    doReturn(true).when(container).isQueueActive(anyString());
    doReturn(preProcessMessageProcessor).when(container).getPreExecutionMessageProcessor();
    doReturn(messageHandler).when(container).getRqueueMessageHandler();
    doReturn(messageConverter).when(messageHandler).getMessageConverter();
    doReturn(rqueueJobDao).when(container).rqueueJobDao();
    doReturn(rqueueStringDao).when(container).rqueueStringDao();
    doThrow(new MessagingException("Failing for some reason."))
        .when(messageHandler)
        .handleMessage(any());
    doReturn(1).when(rqueueConfig).getRetryPerPoll();
    rqueueMessage = new RqueueMessage();
    Message<String> message =
        (Message<String>)
            messageConverter.toMessage("test message", RqueueMessageHeaders.emptyMessageHeaders());
    rqueueMessage.setMessage(message.getPayload());
    rqueueMessage.setQueueName(queueName);
    defaultMessageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
  }

  @Test
  void callDiscardProcessor() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(3).when(rqueueConfig).getRetryPerPoll();
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doAnswer(i -> defaultMessageMetadata).when(rqueueMessageMetadataService).get(anyString());
    new RqueueExecutor(
            containerWeakReference,
            rqueueConfig,
            postProcessingHandler,
            rqueueMessage,
            queueDetail,
            semaphore)
        .run();
    assertEquals(1, discardProcessor.getCount());
  }

  @Test
  void callDeadLetterProcessor() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName, "test-dlq");
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService).get(anyString());
    doReturn(3).when(rqueueConfig).getRetryPerPoll();
    new RqueueExecutor(
            containerWeakReference,
            rqueueConfig,
            postProcessingHandler,
            rqueueMessage,
            queueDetail,
            semaphore)
        .run();
    assertEquals(1, deadLetterProcessor.getCount());
  }

  @Test
  void messageIsParkedForRetry() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService).get(anyString());
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    new RqueueExecutor(
            containerWeakReference,
            rqueueConfig,
            postProcessingHandler,
            rqueueMessage,
            queueDetail,
            semaphore)
        .run();
    verify(messageTemplate, times(1))
        .moveMessage(
            eq(queueDetail.getProcessingQueueName()),
            eq(queueDetail.getDelayedQueueName()),
            eq(rqueueMessage),
            any(),
            eq(5000L));
  }

  @Test
  void messageIsNotExecutedWhenDeletedManually() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    messageMetadata.setDeleted(true);
    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(eq(rqueueMessage));
    new RqueueExecutor(
            containerWeakReference,
            rqueueConfig,
            postProcessingHandler,
            rqueueMessage,
            queueDetail,
            semaphore)
        .run();
    verify(messageHandler, times(0)).handleMessage(any());
  }

  @Test
  void messageIsDeletedWhileExecuting() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    AtomicInteger atomicInteger = new AtomicInteger(0);
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any(RqueueMessage.class));
    doAnswer(
            invocation -> {
              if (atomicInteger.get() == 0) {
                atomicInteger.incrementAndGet();
                return messageMetadata;
              }
              messageMetadata.setDeleted(true);
              return messageMetadata;
            })
        .when(rqueueMessageMetadataService)
        .get(RqueueMessageUtils.getMessageMetaId(queueName, rqueueMessage.getId()));
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    new RqueueExecutor(
            containerWeakReference,
            rqueueConfig,
            postProcessingHandler,
            rqueueMessage,
            queueDetail,
            semaphore)
        .run();
    verify(messageHandler, times(1)).handleMessage(any());
  }

  @Test
  void handleIgnoredMessage() {
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    MessageProcessor messageProcessor =
        new MessageProcessor() {
          @Override
          public boolean process(Object message) {
            return false;
          }
        };
    doReturn(messageProcessor).when(container).getPreExecutionMessageProcessor();
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
            containerWeakReference,
            rqueueConfig,
            postProcessingHandler,
            rqueueMessage,
            queueDetail,
            semaphore)
        .run();
    verify(messageHandler, times(0)).handleMessage(any());
    verify(messageTemplate, times(1))
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
  }

  private class TestMessageProcessor implements MessageProcessor {
    private int count;

    @Override
    public boolean process(Object message) {
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
