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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueMessageMetadataDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.impl.RqueueMessageMetadataServiceImpl;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

@CoreUnitTest
class RqueueMessageMetadataServiceTest extends TestBase {
  @Mock private RqueueMessageMetadataDao rqueueMessageMetadataDao;
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  private final String queueName = "test-queue";

  @BeforeEach
  public void init() {
    rqueueMessageMetadataService = new RqueueMessageMetadataServiceImpl(rqueueMessageMetadataDao);
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
  void deleteMessageWhereMetaInfoNotFound() {
    String id = UUID.randomUUID().toString();
    doAnswer(
            invocation -> {
              MessageMetadata metadata = invocation.getArgument(0);
              assertTrue(metadata.isDeleted());
              assertNotNull(metadata.getDeletedOn());
              return null;
            })
        .when(rqueueMessageMetadataDao)
        .save(any(), eq(Duration.ofDays(7)));
    rqueueMessageMetadataService.deleteMessage(queueName, id, Duration.ofDays(7));
  }

  @Test
  void deleteMessageWhereMetaInfo() {
    String id = UUID.randomUUID().toString();
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
    rqueueMessageMetadataService.deleteMessage(queueName, id, Duration.ofDays(7));
  }
}
