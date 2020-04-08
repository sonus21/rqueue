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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.processor.MessageProcessor;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.GenericMessageConverter;
import org.springframework.messaging.converter.MessageConverter;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MessageExecutorTest {
  private RqueueMessageListenerContainer container = mock(RqueueMessageListenerContainer.class);
  private WeakReference<RqueueMessageListenerContainer> containerWeakReference =
      new WeakReference<>(container);
  private TestMessageProcessor deadLetterProcessor = new TestMessageProcessor();
  private TestMessageProcessor discardProcessor = new TestMessageProcessor();
  private RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
  private RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
  private RqueueMessage rqueueMessage = new RqueueMessage();

  private class TestMessageProcessor implements MessageProcessor {
    private int count;

    @Override
    public void process(Object message) {
      count += 1;
    }

    public void clear() {
      this.count = 0;
    }

    public int getCount() {
      return count;
    }
  }

  @Before
  public void init() {
    List<MessageConverter> messageConverterList =
        Collections.singletonList(new GenericMessageConverter());
    rqueueMessage.setMessage("test message");
    doReturn(deadLetterProcessor).when(container).getDlqMessageProcessor();
    doReturn(discardProcessor).when(container).getDiscardMessageProcessor();
    doReturn(true).when(container).isQueueActive(anyString());
    doReturn(messageTemplate).when(container).getRqueueMessageTemplate();
    doReturn(messageHandler).when(container).getRqueueMessageHandler();
    doReturn(messageConverterList).when(messageHandler).getMessageConverters();
    doThrow(new MessagingException("Failing for some reason."))
        .when(messageHandler)
        .handleMessage(any());
  }

  @Test
  public void callDiscardProcessor() {
    QueueDetail queueDetail = new QueueDetail("test", 3, "", false, 900000);
    MessageExecutor messageExecutor =
        new MessageExecutor(rqueueMessage, queueDetail, containerWeakReference);
    messageExecutor.run();
    assertEquals(1, discardProcessor.getCount());
  }

  @Test
  public void callDeadLetterProcessor() {
    QueueDetail queueDetail = new QueueDetail("test", 3, "dead-test", false, 900000);

    MessageExecutor messageExecutor =
        new MessageExecutor(rqueueMessage, queueDetail, containerWeakReference);
    messageExecutor.run();
    assertEquals(1, deadLetterProcessor.getCount());
  }
}
