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

package com.github.sonus21.rqueue.core.impl;

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.LockCanNotBeAcquired;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.MessageMetadataTestUtils;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueMessageManagerImplTest extends TestBase {

  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final String messageId = UUID.randomUUID().toString();
  private final String message = "Test Message";


  private final String queueName = "test-queue";
  private final String deadLetterQueueName = "dead-test-queue";
  private final MessageMetadata messageMetadata = MessageMetadataTestUtils.createMessageMetadata(
      messageConverter,
      queueName, message);
  private final RqueueMessage rqueueMessage = messageMetadata.getRqueueMessage();
  private final QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);


  private final String queueName2 = "test-queue2";
  private final String priority = "high";
  private final String queueNameWithPriority = PriorityUtils.getQueueNameForPriority(queueName2,
      priority);
  private final QueueDetail queueDetail2 = TestUtils.createQueueDetail(
      queueNameWithPriority,
      Collections.singletonMap(priority, 5), 3, 15_000, "");


  private final MessageMetadata messageMetadata2 = MessageMetadataTestUtils.createMessageMetadata(
      messageConverter,
      queueNameWithPriority, message);
  private final RqueueMessage rqueueMessage2 = messageMetadata2.getRqueueMessage();
  @Mock
  private RqueueLockManager rqueueLockManager;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private MessageSweeper messageSweeper;
  private RqueueMessageManager rqueueMessageManager;


  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueMessageManager =
        new RqueueMessageManagerImpl(
            rqueueMessageTemplate, messageConverter, null);
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
    EndpointRegistry.register(queueDetail2);
    writeField(rqueueMessageManager, "rqueueConfig", rqueueConfig, true);
    writeField(rqueueMessageManager, "rqueueLockManager", rqueueLockManager, true);
    writeField(
        rqueueMessageManager, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
  }


  @Test
  void deleteAllMessages() {
    try (MockedStatic<MessageSweeper> messageSweeperMockedStatic = Mockito.mockStatic(
        MessageSweeper.class)) {
      messageSweeperMockedStatic.when(() -> MessageSweeper.getInstance(any(), any(), any()))
          .thenReturn(messageSweeper);
      rqueueMessageManager.deleteAllMessages(queueName);
      verify(messageSweeper, times(1)).deleteAllMessages(any());
    }
  }

  @Test
  void deleteAllMessagesWithPriority() {
    try (MockedStatic<MessageSweeper> messageSweeperMockedStatic = Mockito.mockStatic(
        MessageSweeper.class)) {
      messageSweeperMockedStatic.when(() -> MessageSweeper.getInstance(any(), any(), any()))
          .thenReturn(messageSweeper);
      rqueueMessageManager.deleteAllMessages(queueName2, priority);
      verify(messageSweeper, times(1)).deleteAllMessages(any());
    }
  }


  @Test
  void getAllMessages() {
    doReturn(Collections.emptyList()).when(rqueueMessageTemplate)
        .getAllMessages(queueDetail.getQueueName(), queueDetail.getProcessingQueueName(),
            queueDetail.getScheduledQueueName());
    assertEquals(0, rqueueMessageManager.getAllMessages(queueName).size());
    doReturn(Collections.singletonList(rqueueMessage)).when(rqueueMessageTemplate)
        .getAllMessages(queueDetail.getQueueName(), queueDetail.getProcessingQueueName(),
            queueDetail.getScheduledQueueName());
    List<Object> messages = rqueueMessageManager.getAllMessages(queueName);
    assertEquals(1, messages.size());
    assertEquals(message, messages.get(0));
  }

  @Test
  void getAllMessagesWithPriority() {
    doReturn(Collections.emptyList()).when(rqueueMessageTemplate)
        .getAllMessages(queueDetail2.getQueueName(), queueDetail2.getProcessingQueueName(),
            queueDetail2.getScheduledQueueName());
    assertEquals(0, rqueueMessageManager.getAllMessages(queueName2, priority).size());

    doReturn(Collections.singletonList(rqueueMessage2)).when(rqueueMessageTemplate)
        .getAllMessages(queueDetail2.getQueueName(), queueDetail2.getProcessingQueueName(),
            queueDetail2.getScheduledQueueName());
    List<Object> messages = rqueueMessageManager.getAllMessages(queueName2, priority);
    assertEquals(1, messages.size());
    assertEquals(message, messages.get(0));
  }

  @Test
  void getRqueueMessage() {
    assertNull(rqueueMessageManager.getRqueueMessage(queueName, messageId));
    verify(rqueueMessageMetadataService, times(1)).getByMessageId(anyString(), anyString());

    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueName, messageId);
    assertEquals(rqueueMessage, rqueueMessageManager.getRqueueMessage(queueName, messageId));
  }

  @Test
  void getRqueueMessageWithPriority() {
    assertNull(rqueueMessageManager.getRqueueMessage(queueName2, priority, messageId));
    verify(rqueueMessageMetadataService, times(1)).getByMessageId(anyString(), anyString());

    doReturn(messageMetadata2)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueNameWithPriority, messageId);
    assertEquals(rqueueMessage2,
        rqueueMessageManager.getRqueueMessage(queueName2, priority, messageId));
  }


  @Test
  void getAllRqueueMessage() {
    assertEquals(0, rqueueMessageManager.getAllRqueueMessage(queueName).size());
    verify(rqueueMessageTemplate, times(1)).getAllMessages(queueDetail.getQueueName(),
        queueDetail.getProcessingQueueName(), queueDetail.getScheduledQueueName());

    doReturn(Collections.singletonList(rqueueMessage))
        .when(rqueueMessageTemplate).getAllMessages(queueDetail.getQueueName(),
            queueDetail.getProcessingQueueName(), queueDetail.getScheduledQueueName());
    List<RqueueMessage> messages = rqueueMessageManager.getAllRqueueMessage(queueName);
    assertEquals(1, messages.size());
    assertEquals(rqueueMessage, messages.get(0));
  }

  @Test
  void getAllRqueueMessageWithPriority() {
    assertEquals(0, rqueueMessageManager.getAllRqueueMessage(queueName2, priority).size());
    verify(rqueueMessageTemplate, times(1)).getAllMessages(queueDetail2.getQueueName(),
        queueDetail2.getProcessingQueueName(), queueDetail2.getScheduledQueueName());

    doReturn(Collections.singletonList(rqueueMessage))
        .when(rqueueMessageTemplate).getAllMessages(queueDetail2.getQueueName(),
            queueDetail2.getProcessingQueueName(), queueDetail2.getScheduledQueueName());
    List<RqueueMessage> messages = rqueueMessageManager.getAllRqueueMessage(queueName2, priority);
    assertEquals(1, messages.size());
    assertEquals(rqueueMessage, messages.get(0));
  }


  @Test
  void getMessage() {
    assertNull(rqueueMessageManager.getMessage(queueName, messageId));
    verify(rqueueMessageMetadataService, times(1)).getByMessageId(anyString(), anyString());

    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueName, messageId);
    assertNotNull(rqueueMessageManager.getMessage(queueName, messageId));
  }

  @Test
  void getMessageWithPriority() {
    assertNull(rqueueMessageManager.getMessage(queueName2, priority, messageId));
    verify(rqueueMessageMetadataService, times(1)).getByMessageId(anyString(), anyString());
    assertNull(rqueueMessageManager.getRqueueMessage(queueName2, priority, messageId));
    verify(rqueueMessageMetadataService, times(2)).getByMessageId(anyString(), anyString());
    doReturn(messageMetadata2)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueNameWithPriority, messageId);
    assertNotNull(rqueueMessageManager.getMessage(queueName2, priority, messageId));
  }

  @Test
  void exist() {
    doReturn(false).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    assertThrows(LockCanNotBeAcquired.class,
        () -> rqueueMessageManager.exist(queueName, messageId));

    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(messageMetadata)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueName, messageId);
    assertTrue(rqueueMessageManager.exist(queueName, messageId));

    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(null)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueName, messageId);
    assertFalse(rqueueMessageManager.exist(queueName, messageId));

    doReturn(false).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    assertThrows(LockCanNotBeAcquired.class,
        () -> rqueueMessageManager.exist(queueName2, priority, messageId));
  }

  @Test
  void existWithPriority() {
    // entry wont exist
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    assertFalse(rqueueMessageManager.exist(queueName2, priority, messageId));

    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(messageMetadata2)
        .when(rqueueMessageMetadataService)
        .getByMessageId(queueNameWithPriority, messageId);
    assertTrue(rqueueMessageManager.exist(queueName2, priority, messageId));
  }


  @Test
  void deleteMessage() {
    assertFalse(rqueueMessageManager.deleteMessage(queueName, messageId));

    doReturn(Duration.ofSeconds(500)).when(rqueueConfig).getMessageDurability(0L);
    doReturn(messageMetadata).when(rqueueMessageMetadataService)
        .getByMessageId(queueName, messageId);
    assertFalse(rqueueMessageManager.deleteMessage(queueName, messageId));

    doReturn(true).when(rqueueMessageMetadataService)
        .deleteMessage(queueName, messageId, Duration.ofSeconds(500));
    assertTrue(rqueueMessageManager.deleteMessage(queueName, messageId));
  }

  @Test
  void deleteMessageWithPriority() {

    assertFalse(rqueueMessageManager.deleteMessage(queueName2, messageId));

    doReturn(Duration.ofSeconds(500)).when(rqueueConfig).getMessageDurability(0L);
    doReturn(messageMetadata2).when(rqueueMessageMetadataService)
        .getByMessageId(queueNameWithPriority, messageId);
    assertFalse(rqueueMessageManager.deleteMessage(queueName2, priority, messageId));

    doReturn(true).when(rqueueMessageMetadataService)
        .deleteMessage(queueNameWithPriority, messageId, Duration.ofSeconds(500));
    assertTrue(rqueueMessageManager.deleteMessage(queueName2, priority, messageId));
  }

  @Test
  void getMessageConverter() {
    assertEquals(messageConverter.hashCode(),
        rqueueMessageManager.getMessageConverter().hashCode());
  }


  @Test
  void moveMessageFromQueueExceptions() {
    assertThrows(IllegalArgumentException.class,
        () -> rqueueMessageManager.moveMessageFromDeadLetterToQueue(null, queueName, null));
    assertThrows(IllegalArgumentException.class,
        () -> rqueueMessageManager.moveMessageFromDeadLetterToQueue(deadLetterQueueName, null,
            null));
    doReturn(new MessageMoveResult(10, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList(anyString(), anyString(), anyInt());
    rqueueMessageManager.moveMessageFromDeadLetterToQueue(deadLetterQueueName, queueName, null);
  }

  @Test
  void moveMessageFromDeadLetterToQueueDefaultSize() {
    doReturn(new MessageMoveResult(100, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList(anyString(), anyString(), anyInt());
    rqueueMessageManager.moveMessageFromDeadLetterToQueue(deadLetterQueueName, queueName, null);
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
    rqueueMessageManager.moveMessageFromDeadLetterToQueue(deadLetterQueueName, queueName, 10);
    verify(rqueueMessageTemplate, times(1))
        .moveMessageListToList(eq(deadLetterQueueName), eq(queueName), anyInt());
    verify(rqueueMessageTemplate, times(1))
        .moveMessageListToList(eq(deadLetterQueueName), eq(queueName), eq(10));
  }
}
