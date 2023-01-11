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

package com.github.sonus21.rqueue.core.impl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessageManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.LockCanNotBeAcquired;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.util.UUID;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueMessageManagerImplTest extends TestBase {

  private final String messageId = UUID.randomUUID().toString();
  private final String queue = "test-queue";
  MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();
  @Mock
  private RqueueMessageTemplate messageTemplate;
  @Mock
  private RqueueLockManager rqueueLockManager;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  private RqueueMessageManager rqueueMessageManager;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueMessageManager =
        new RqueueMessageManagerImpl(messageTemplate, messageConverter, messageHeaders);
    FieldUtils.writeField(
        rqueueMessageManager, "rqueueMessageMetadataService", rqueueMessageMetadataService, true);
    FieldUtils.writeField(rqueueMessageManager, "rqueueLockManager", rqueueLockManager, true);
  }

  @Test
  void getMessageDoesNotExist() {
    assertNull(rqueueMessageManager.getMessage(queue, messageId));
    verify(rqueueMessageMetadataService, times(1)).getByMessageId(anyString(), anyString());
  }

  @Test
  void getRqueueMessageDoesNotExist() {
    assertNull(rqueueMessageManager.getRqueueMessage(queue, messageId));
    verify(rqueueMessageMetadataService, times(1)).getByMessageId(anyString(), anyString());
  }

  @Test
  void getMessageExist() {
    doReturn(TestUtils.createMessageMetadata(messageConverter, queue))
        .when(rqueueMessageMetadataService)
        .getByMessageId(queue, messageId);
    assertNotNull(rqueueMessageManager.getMessage(queue, messageId));
  }

  @Test
  void existLockCanNotBeAcquired() {
    doReturn(false).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    assertThrows(LockCanNotBeAcquired.class, () -> rqueueMessageManager.exist(queue, messageId));
  }

  @Test
  void existLockAcquiredAndMessageExist() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    doReturn(TestUtils.createMessageMetadata(messageConverter, queue))
        .when(rqueueMessageMetadataService)
        .getByMessageId(queue, messageId);
    assertTrue(rqueueMessageManager.exist(queue, messageId));
  }

  @Test
  void existLockAcquiredAndMessageDoesNotExist() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), anyString(), any());
    assertFalse(rqueueMessageManager.exist(queue, messageId));
  }
}
