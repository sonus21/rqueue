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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.core.QueueRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageSender;
import com.github.sonus21.rqueue.core.RqueueMessageSenderImpl;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.TestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageSenderTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  private RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
  private RqueueMessageSender rqueueMessageSender =
      new RqueueMessageSenderImpl(rqueueMessageTemplate);
  private String queueName = "test-queue";
  private QueueDetail queueDetail = TestUtils.createQueueDetail(queueName, 3, true, 90000L, null);
  private String slowQueue = "slow-queue";
  private String deadLetterQueueName = "dead-test-queue";
  private String message = "Test Message";

  @Before
  public void init() {
    QueueRegistry.register(queueDetail);
  }

  @Test
  public void putWithNullQueueName() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("queueName cannot be null");
    rqueueMessageSender.put(null, null);
  }

  @Test
  public void putWithNullMessage() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("message cannot be null");
    rqueueMessageSender.put(queueName, null);
  }

  @Test
  public void put() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.put(queueName, message));
  }

  @Test
  public void putWithRetry() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.put(queueName, message, 3));
  }

  @Test
  public void putWithDelay() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(
            eq(queueDetail.getDelayedQueueName()),
            eq(queueDetail.getDelayedQueueChannelName()),
            any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.put(queueName, message, 1000L));
  }

  @Test
  public void putWithDelayAndRetry() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(
            eq(queueDetail.getDelayedQueueName()),
            eq(queueDetail.getDelayedQueueChannelName()),
            any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.put(queueName, message, 3, 1000L));
  }

  @Test
  public void moveMessageFromQueueExceptions() {
    // source is not provided
    try {
      rqueueMessageSender.moveMessageFromDeadLetterToQueue(null, queueName, null);
      fail();
    } catch (IllegalArgumentException e) {
    }

    // destination is not provided
    try {
      rqueueMessageSender.moveMessageFromDeadLetterToQueue(queueName, null, null);
      fail();
    } catch (IllegalArgumentException e) {
    }
    doReturn(new MessageMoveResult(10, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList(anyString(), anyString(), anyInt());
    rqueueMessageSender.moveMessageFromDeadLetterToQueue(queueName, slowQueue, null);
  }

  @Test
  public void moveMessageFromDeadLetterToQueueDefaultSize() {
    doReturn(new MessageMoveResult(100, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList(anyString(), anyString(), anyInt());
    rqueueMessageSender.moveMessageFromDeadLetterToQueue(deadLetterQueueName, queueName, null);
    verify(rqueueMessageTemplate, times(1))
        .moveMessageListToList(eq(deadLetterQueueName), eq(queueName), anyInt());
    verify(rqueueMessageTemplate, times(1))
        .moveMessageListToList(deadLetterQueueName, queueName, 100);
  }

  @Test
  public void moveMessageFromDeadLetterToQueueFixedSize() {
    doReturn(new MessageMoveResult(10, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList(anyString(), anyString(), anyInt());
    rqueueMessageSender.moveMessageFromDeadLetterToQueue(deadLetterQueueName, queueName, 10);

    verify(rqueueMessageTemplate, times(1))
        .moveMessageListToList(eq(deadLetterQueueName), eq(queueName), anyInt());
    verify(rqueueMessageTemplate, times(1))
        .moveMessageListToList(eq(deadLetterQueueName), eq(queueName), eq(10));
  }
}
