/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
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
    RqueueMessage[] rqueueMessages = new RqueueMessage[1];
    doReturn(MessageBuilder.withPayload(message).build())
        .when(messageConverter)
        .toMessage(message, null);
    doAnswer(
            invocation -> {
              rqueueMessages[0] = (RqueueMessage) invocation.getArguments()[1];
              return null;
            })
        .when(rqueueMessageTemplate)
        .add(anyString(), any(RqueueMessage.class));
    assertTrue(messageWriter.pushMessage(queueName, message, null, null));
    RqueueMessage rqueueMessage = rqueueMessages[0];
    assertEquals(message, rqueueMessage.getMessage());
    assertNull(rqueueMessage.getRetryCount());
    assertEquals(0, rqueueMessage.getProcessAt());
    assertNull(rqueueMessage.getId());
    assertEquals(queueName, rqueueMessage.getQueueName());
    assertNull(rqueueMessage.getAccessTime());
    assertTrue(rqueueMessage.getQueuedTime() <= System.currentTimeMillis());
    assertNull(rqueueMessage.getReEnqueuedAt());
  }

  @Test
  public void pushMessageWithLessThanOneSecondDelayAndRetryCount() {
    String message = "Test Message";
    String queueName = "test-queue";
    RqueueMessage[] rqueueMessages = new RqueueMessage[1];
    doReturn(MessageBuilder.withPayload(message).build())
        .when(messageConverter)
        .toMessage(message, null);
    doAnswer(
            invocation -> {
              rqueueMessages[0] = (RqueueMessage) invocation.getArguments()[1];
              return null;
            })
        .when(rqueueMessageTemplate)
        .add(anyString(), any(RqueueMessage.class));
    assertTrue(messageWriter.pushMessage(queueName, message, 3, 100L));
    RqueueMessage rqueueMessage = rqueueMessages[0];
    assertEquals(message, rqueueMessage.getMessage());
    assertEquals((Integer) 3, rqueueMessage.getRetryCount());
    assertTrue(System.currentTimeMillis() + 100 >= rqueueMessage.getProcessAt());
    assertNotNull(rqueueMessage.getId());
    assertEquals(queueName, rqueueMessage.getQueueName());
    assertNull(rqueueMessage.getAccessTime());
    assertTrue(rqueueMessage.getQueuedTime() <= System.currentTimeMillis());
    assertNull(rqueueMessage.getReEnqueuedAt());
  }

  @Test
  public void pushMessageWithDelayAndRetryCount() {
    String message = "Test Message";
    String queueName = "test-queue";
    RqueueMessage[] rqueueMessages = new RqueueMessage[1];
    doReturn(MessageBuilder.withPayload(message).build())
        .when(messageConverter)
        .toMessage(message, null);
    doAnswer(
            invocation -> {
              rqueueMessages[0] = (RqueueMessage) invocation.getArguments()[1];
              return null;
            })
        .when(rqueueMessageTemplate)
        .addToZset(anyString(), any(RqueueMessage.class));
    assertTrue(messageWriter.pushMessage(queueName, message, 3, 1200L));
    RqueueMessage rqueueMessage = rqueueMessages[0];
    assertEquals(message, rqueueMessage.getMessage());
    assertEquals((Integer) 3, rqueueMessage.getRetryCount());
    assertTrue(System.currentTimeMillis() + 1200 >= rqueueMessage.getProcessAt());
    assertNotNull(rqueueMessage.getId());
    assertEquals(queueName, rqueueMessage.getQueueName());
    assertNull(rqueueMessage.getAccessTime());
    assertTrue(rqueueMessage.getQueuedTime() <= System.currentTimeMillis());
    assertNull(rqueueMessage.getReEnqueuedAt());
  }
}
