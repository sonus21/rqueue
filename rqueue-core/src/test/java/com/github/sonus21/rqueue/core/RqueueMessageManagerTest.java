/*
 * Copyright (c) 2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.impl.RqueueMessageManagerImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class RqueueMessageManagerTest {

  private final String queueName = "test-queue";
  private final QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
  private final String slowQueue = "slow-queue";
  private final String deadLetterQueueName = "dead-test-queue";
  private final String message = "Test Message";
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueLockManager rqueueLockManager;

  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueMessageManager rqueueMessageManager;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueMessageManager =
        new RqueueMessageManagerImpl(
            rqueueMessageTemplate, new DefaultRqueueMessageConverter(), null);
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
    writeField(rqueueMessageManager, "rqueueConfig", rqueueConfig, true);
    writeField(rqueueMessageManager, "rqueueLockManager", rqueueLockManager, true);
    writeField(
        rqueueMessageManager, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
  }

  @Test
  void deleteAllMessages() {
  }

  @Test
  void testDeleteAllMessages() {
  }

  @Test
  void getAllMessages() {
  }

  @Test
  void testGetAllMessages() {
  }

  @Test
  void getMessage() {
  }

  @Test
  void testGetMessage() {
  }

  @Test
  void exist() {
  }

  @Test
  void testExist() {
  }

  @Test
  void getRqueueMessage() {
  }

  @Test
  void testGetRqueueMessage() {
  }

  @Test
  void getAllRqueueMessage() {
  }

  @Test
  void testGetAllRqueueMessage() {
  }

  @Test
  void deleteMessage() {
  }

  @Test
  void testDeleteMessage() {
  }

  @Test
  void getMessageConverter() {
  }


  @Test
  void moveMessageFromQueueExceptions() {
    // source is not provided
    try {
      rqueueMessageManager.moveMessageFromDeadLetterToQueue(null, queueName, null);
      fail();
    } catch (IllegalArgumentException e) {
    }

    // destination is not provided
    try {
      rqueueMessageManager.moveMessageFromDeadLetterToQueue(queueName, null, null);
      fail();
    } catch (IllegalArgumentException e) {
    }
    doReturn(new MessageMoveResult(10, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList(anyString(), anyString(), anyInt());
    rqueueMessageManager.moveMessageFromDeadLetterToQueue(queueName, slowQueue, null);
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