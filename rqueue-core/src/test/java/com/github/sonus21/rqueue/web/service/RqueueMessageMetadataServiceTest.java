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

package com.github.sonus21.rqueue.web.service;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueMessageMetadataDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.impl.RqueueMessageMetadataServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@CoreUnitTest
class RqueueMessageMetadataServiceTest extends TestBase {

  private final String queueName = "test-queue";
  @Mock private RqueueMessageMetadataDao rqueueMessageMetadataDao;
  @Mock private RqueueStringDao rqueueStringDao;
  @Mock private RqueueLockManager lockManager;
  private RqueueMessageMetadataService rqueueMessageMetadataService;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueMessageMetadataService =
        new RqueueMessageMetadataServiceImpl(
            rqueueMessageMetadataDao, rqueueStringDao, lockManager);
  }

  @Test
  void get() {
    String id = UUID.randomUUID().toString();
    String msgId = RqueueMessageUtils.getMessageMetaId(queueName, id);
    MessageMetadata metadata = new MessageMetadata(id, MessageStatus.ENQUEUED);
    metadata.setDeleted(true);
    doReturn(null).when(rqueueMessageMetadataDao).get(msgId);
    assertNull(rqueueMessageMetadataService.get(msgId));
    doReturn(metadata).when(rqueueMessageMetadataDao).get(msgId);
    assertEquals(metadata, rqueueMessageMetadataService.get(msgId));
  }

  @Test
  void findAll() {
    String id = UUID.randomUUID().toString();
    String msgId = RqueueMessageUtils.getMessageMetaId(queueName, id);
    MessageMetadata metadata = new MessageMetadata(id, MessageStatus.ENQUEUED);
    metadata.setDeleted(true);
    List<String> ids = Arrays.asList(msgId, UUID.randomUUID().toString());
    doReturn(Arrays.asList(metadata, null)).when(rqueueMessageMetadataDao).findAll(ids);
    assertEquals(Collections.singletonList(metadata), rqueueMessageMetadataService.findAll(ids));
  }

  @Test
  void deleteMessageShouldCreateMessageMetadata() {
    String id = UUID.randomUUID().toString();
    doReturn(true).when(lockManager).acquireLock(eq(id), anyString(), eq(Duration.ofSeconds(1)));
    doAnswer(
            invocation -> {
              MessageMetadata metadata = invocation.getArgument(0);
              assertTrue(metadata.isDeleted());
              assertNotNull(metadata.getDeletedOn());
              return null;
            })
        .when(rqueueMessageMetadataDao)
        .save(any(), eq(Duration.ofDays(7)));
    assertTrue(rqueueMessageMetadataService.deleteMessage(queueName, id, Duration.ofDays(7)));
  }

  @Test
  void deleteMessage() {
    String id = UUID.randomUUID().toString();
    doReturn(true).when(lockManager).acquireLock(eq(id), anyString(), eq(Duration.ofSeconds(1)));
    MessageMetadata metadata =
        new MessageMetadata(
            RqueueMessageUtils.getMessageMetaId(queueName, id), MessageStatus.ENQUEUED);
    metadata.setDeleted(false);
    doReturn(metadata)
        .when(rqueueMessageMetadataDao)
        .get(RqueueMessageUtils.getMessageMetaId(queueName, id));
    doAnswer(
            invocation -> {
              MessageMetadata metadataBeingSaved = invocation.getArgument(0);
              assertTrue(metadataBeingSaved.isDeleted());
              assertNotNull(metadataBeingSaved.getDeletedOn());
              return null;
            })
        .when(rqueueMessageMetadataDao)
        .save(any(), eq(Duration.ofDays(7)));
    assertTrue(rqueueMessageMetadataService.deleteMessage(queueName, id, Duration.ofDays(7)));
  }

  @Test
  void deleteMessageShouldFailDueToLock() {
    String id = UUID.randomUUID().toString();
    doReturn(false).when(lockManager).acquireLock(eq(id), anyString(), eq(Duration.ofSeconds(1)));
    assertFalse(rqueueMessageMetadataService.deleteMessage(queueName, id, Duration.ofDays(7)));
    verifyNoInteractions(rqueueMessageMetadataDao);
  }
}
