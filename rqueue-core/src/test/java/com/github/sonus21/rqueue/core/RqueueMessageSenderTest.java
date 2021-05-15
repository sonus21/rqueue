/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core;

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.impl.RqueueMessageSenderImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueMessageSenderTest extends TestBase {
  @Mock private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock private RqueueConfig rqueueConfig;
  @Mock private RqueueMessageMetadataService rqueueMessageMetadataService;
  private RqueueMessageSender rqueueMessageSender;
  private final String queueName = "test-queue";
  private final QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
  private final String slowQueue = "slow-queue";
  private final String deadLetterQueueName = "dead-test-queue";
  private final String message = "Test Message";

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueMessageSender =
        new RqueueMessageSenderImpl(
            rqueueMessageTemplate, new DefaultRqueueMessageConverter(), null);
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
    writeField(rqueueMessageSender, "rqueueConfig", rqueueConfig, true);
    writeField(
        rqueueMessageSender, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
  }

  @Test
  void putWithNullQueueName() {
    assertThrows(IllegalArgumentException.class, () -> rqueueMessageSender.enqueue(null, null));
  }

  @Test
  void putWithNullMessage() {
    assertThrows(
        IllegalArgumentException.class, () -> rqueueMessageSender.enqueue(queueName, null));
  }

  @Test
  void put() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.enqueue(queueName, message));
  }

  @Test
  void putWithRetry() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessage(eq(queueDetail.getQueueName()), any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.enqueueWithRetry(queueName, message, 3));
  }

  @Test
  void putWithDelay() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(
            eq(queueDetail.getDelayedQueueName()),
            eq(queueDetail.getDelayedQueueChannelName()),
            any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.enqueueIn(queueName, message, 1000L));
  }

  @Test
  void putWithDelayAndRetry() {
    doReturn(1L)
        .when(rqueueMessageTemplate)
        .addMessageWithDelay(
            eq(queueDetail.getDelayedQueueName()),
            eq(queueDetail.getDelayedQueueChannelName()),
            any(RqueueMessage.class));
    assertTrue(rqueueMessageSender.enqueueInWithRetry(queueName, message, 3, 1000L));
  }

  @Test
  void moveMessageFromQueueExceptions() {
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
  void moveMessageFromDeadLetterToQueueDefaultSize() {
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
  void moveMessageFromDeadLetterToQueueFixedSize() {
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
