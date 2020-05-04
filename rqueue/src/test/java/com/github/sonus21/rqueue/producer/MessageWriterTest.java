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

package com.github.sonus21.rqueue.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.support.MessageBuilder;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MessageWriterTest {
  private RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
  private CompositeMessageConverter messageConverter = mock(CompositeMessageConverter.class);
  private MessageWriter messageWriter = new MessageWriter(rqueueMessageTemplate, messageConverter);

  @Test
  public void pushMessageWithNoDelay() {
    String message = "Test Message";
    String queueName = "test-queue";
    List<RqueueMessage> rqueueMessages = new ArrayList<>();
    doReturn(MessageBuilder.withPayload(message).build())
        .when(messageConverter)
        .toMessage(message, null);
    doAnswer(
            invocation -> {
              rqueueMessages.add(invocation.getArgument(1));
              return null;
            })
        .when(rqueueMessageTemplate)
        .addMessage(anyString(), any(RqueueMessage.class));
    assertTrue(messageWriter.pushMessage(queueName, message, null, null));
    assertEquals(1, rqueueMessages.size());
    RqueueMessage rqueueMessage = rqueueMessages.get(0);
    assertEquals(message, rqueueMessage.getMessage());
    assertNull(rqueueMessage.getRetryCount());
    assertEquals(rqueueMessage.getQueuedTime(), rqueueMessage.getProcessAt());
    assertEquals(queueName, rqueueMessage.getQueueName());
    assertTrue(rqueueMessage.getQueuedTime() <= System.currentTimeMillis());
    assertNull(rqueueMessage.getReEnqueuedAt());
  }

  @Test
  public void pushMessageWithLessThanOneSecondDelayAndRetryCount() {
    String message = "Test Message";
    String queueName = "test-queue";
    List<RqueueMessage> rqueueMessages = new ArrayList<>();
    doReturn(MessageBuilder.withPayload(message).build())
        .when(messageConverter)
        .toMessage(message, null);
    doAnswer(
            invocation -> {
              rqueueMessages.add(invocation.getArgument(1));
              return null;
            })
        .when(rqueueMessageTemplate)
        .addMessage(anyString(), any(RqueueMessage.class));
    assertTrue(messageWriter.pushMessage(queueName, message, 3, 100L));
    assertEquals(1, rqueueMessages.size());
    RqueueMessage rqueueMessage = rqueueMessages.get(0);
    assertEquals(message, rqueueMessage.getMessage());
    assertEquals((Integer) 3, rqueueMessage.getRetryCount());
    assertTrue(System.currentTimeMillis() + 100 >= rqueueMessage.getProcessAt());
    assertEquals(queueName, rqueueMessage.getQueueName());
    assertTrue(rqueueMessage.getQueuedTime() <= System.currentTimeMillis());
    assertNull(rqueueMessage.getReEnqueuedAt());
  }

  @Test
  public void pushMessageWithDelayAndRetryCount() {
    String message = "Test Message";
    String queueName = "test-queue";
    List<RqueueMessage> rqueueMessages = new ArrayList<>();
    doReturn(MessageBuilder.withPayload(message).build())
        .when(messageConverter)
        .toMessage(message, null);
    doAnswer(
            invocation -> {
              rqueueMessages.add(invocation.getArgument(1));
              return null;
            })
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(anyString(), any(RqueueMessage.class));
    assertTrue(messageWriter.pushMessage(queueName, message, 3, 1200L));
    assertEquals(1, rqueueMessages.size());
    RqueueMessage rqueueMessage = rqueueMessages.get(0);
    assertEquals(message, rqueueMessage.getMessage());
    assertEquals((Integer) 3, rqueueMessage.getRetryCount());
    assertTrue(System.currentTimeMillis() + 1200 >= rqueueMessage.getProcessAt());
    assertEquals(queueName, rqueueMessage.getQueueName());
    assertTrue(rqueueMessage.getQueuedTime() <= System.currentTimeMillis());
    assertNull(rqueueMessage.getReEnqueuedAt());
  }
}
