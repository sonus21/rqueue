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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConverter;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueExecutorTest {
  private RqueueMessageListenerContainer container = mock(RqueueMessageListenerContainer.class);
  private WeakReference<RqueueMessageListenerContainer> containerWeakReference =
      new WeakReference<>(container);
  private RqueueWebConfig rqueueWebConfig = new RqueueWebConfig();
  private RqueueMessageMetadataService rqueueMessageMetadataService =
      mock(RqueueMessageMetadataService.class);
  private TestMessageProcessor deadLetterProcessor = new TestMessageProcessor();
  private TestMessageProcessor discardProcessor = new TestMessageProcessor();
  private TestMessageProcessor preProcessMessageProcessor = new TestMessageProcessor();
  private RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
  private RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
  private RqueueMessage rqueueMessage = new RqueueMessage();
  private Semaphore semaphore = new Semaphore(100);
  private MessageConverter messageConverter  = new CompositeMessageConverter( Collections.singletonList(new GenericMessageConverter()));
  private int retryPerPoll = 3;
  private TaskExecutionBackOff taskBackOff = new FixedTaskExecutionBackOff();

  @Before
  public void init() throws IllegalAccessException {
    rqueueMessage.setMessage("test message");
    rqueueMessage.setId(UUID.randomUUID().toString());
    doReturn(rqueueWebConfig).when(container).getRqueueWebConfig();
    doReturn(rqueueMessageMetadataService).when(container).getRqueueMessageMetadataService();
    doReturn(deadLetterProcessor).when(container).getDeadLetterQueueMessageProcessor();
    doReturn(discardProcessor).when(container).getDiscardMessageProcessor();
    doReturn(true).when(container).isQueueActive(anyString());
    doReturn(messageTemplate).when(container).getRqueueMessageTemplate();
    doReturn(messageConverter).when(messageHandler).getMessageConverter();
    doReturn(preProcessMessageProcessor).when(container).getPreExecutionMessageProcessor();
    doThrow(new MessagingException("Failing for some reason."))
        .when(messageHandler)
        .handleMessage(any());
  }

  @Test
  public void callDiscardProcessor() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    RqueueExecutor rqueueExecutor =
        new RqueueExecutor(
            rqueueMessage,
            queueDetail,
            semaphore,
            containerWeakReference,
            messageHandler,
            10,
            taskBackOff);
    rqueueExecutor.run();
    assertEquals(1, discardProcessor.getCount());
  }

  @Test
  public void callDeadLetterProcessor() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test", "test-dlq");
    RqueueExecutor rqueueExecutor =
        new RqueueExecutor(
            rqueueMessage,
            queueDetail,
            semaphore,
            containerWeakReference,
            messageHandler,
            -1,
            taskBackOff);
    rqueueExecutor.run();
    assertEquals(1, deadLetterProcessor.getCount());
  }

  @Test
  public void messageIsParkedForRetry() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    RqueueExecutor rqueueExecutor =
        new RqueueExecutor(
            rqueueMessage,
            queueDetail,
            semaphore,
            containerWeakReference,
            messageHandler,
            1,
            taskBackOff);
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    rqueueExecutor.run();
    verify(messageTemplate, times(1))
        .moveMessage(
            eq(queueDetail.getProcessingQueueName()),
            eq(queueDetail.getDelayedQueueName()),
            eq(rqueueMessage),
            any(),
            eq(5000L));
  }

  @Test
  public void messageIsNotExecutedWhenDeletedManually() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    RqueueExecutor rqueueExecutor =
        new RqueueExecutor(
            rqueueMessage,
            queueDetail,
            semaphore,
            containerWeakReference,
            messageHandler,
            retryPerPoll,
            taskBackOff);
    MessageMetadata messageMetadata = new MessageMetadata();
    messageMetadata.setDeleted(true);
    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .get("__rq::m-mdata::" + rqueueMessage.getId());
    rqueueExecutor.run();
    verify(messageHandler, times(0)).handleMessage(any());
  }

  @Test
  public void messageIsDeletedWhileExecuting() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    RqueueExecutor rqueueExecutor =
        new RqueueExecutor(
            rqueueMessage,
            queueDetail,
            semaphore,
            containerWeakReference,
            messageHandler,
            1,
            taskBackOff);
    AtomicInteger atomicInteger = new AtomicInteger(0);
    doAnswer(
            invocation -> {
              if (atomicInteger.get() == 0) {
                atomicInteger.incrementAndGet();
                return null;
              }
              MessageMetadata messageMetadata = new MessageMetadata();
              messageMetadata.setDeleted(true);
              return messageMetadata;
            })
        .when(rqueueMessageMetadataService)
        .get("__rq::m-mdata::" + rqueueMessage.getId());
    doThrow(new MessagingException("Failing on purpose")).when(messageHandler).handleMessage(any());
    rqueueExecutor.run();
    verify(messageHandler, times(1)).handleMessage(any());
  }

  @Test
  public void handleIgnoredMessage() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    MessageProcessor messageProcessor =
        new MessageProcessor() {
          @Override
          public boolean process(Object message) {
            return false;
          }
        };
    doReturn(messageProcessor).when(container).getPreExecutionMessageProcessor();

    RqueueExecutor rqueueExecutor =
        new RqueueExecutor(
            rqueueMessage,
            queueDetail,
            semaphore,
            containerWeakReference,
            messageHandler,
            1,
            taskBackOff);
    rqueueExecutor.run();
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
