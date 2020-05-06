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

package com.github.sonus21.rqueue.web.service;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.web.service.impl.RqueueMessageMetadataServiceImpl;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageMetadataServiceTest {
  private RqueueRedisTemplate<MessageMetadata> rqueueRedisTemplate =
      mock(RqueueRedisTemplate.class);
  private RqueueMessageMetadataService rqueueMessageMetadataService =
      new RqueueMessageMetadataServiceImpl(rqueueRedisTemplate);

  @Test
  public void get() {
    String id = UUID.randomUUID().toString();
    String msgId = MessageUtils.getMessageMetaId(id);
    MessageMetadata metadata = new MessageMetadata(id);
    metadata.setDeleted(true);
    doReturn(null).when(rqueueRedisTemplate).get(msgId);
    assertNull(rqueueMessageMetadataService.get(msgId));
    doReturn(metadata).when(rqueueRedisTemplate).get(msgId);
    assertEquals(metadata, rqueueMessageMetadataService.get(msgId));
  }

  @Test
  public void findAll() {
    String id = UUID.randomUUID().toString();
    String msgId = MessageUtils.getMessageMetaId(id);
    MessageMetadata metadata = new MessageMetadata(id);
    metadata.setDeleted(true);
    List<String> ids = Arrays.asList(msgId, UUID.randomUUID().toString());
    doReturn(Arrays.asList(metadata, null)).when(rqueueRedisTemplate).mget(ids);
    assertEquals(Collections.singletonList(metadata), rqueueMessageMetadataService.findAll(ids));
  }

  @Test
  public void deleteMessageWhereMetaInfoNotFound() {
    String id = UUID.randomUUID().toString();
    doAnswer(
            invocation -> {
              MessageMetadata metadata = invocation.getArgument(1);
              assertTrue(metadata.isDeleted());
              assertNotNull(metadata.getDeletedOn());
              return null;
            })
        .when(rqueueRedisTemplate)
        .set(eq(MessageUtils.getMessageMetaId(id)), any(), eq(Duration.ofDays(7)));
    rqueueMessageMetadataService.deleteMessage(id, Duration.ofDays(7));
  }

  @Test
  public void deleteMessageWhereMetaInfo() {
    String id = UUID.randomUUID().toString();
    MessageMetadata metadata = new MessageMetadata(id);
    metadata.setDeleted(false);
    doReturn(metadata).when(rqueueRedisTemplate).get(MessageUtils.getMessageMetaId(id));
    doAnswer(
            invocation -> {
              MessageMetadata metadataBeingSaved = invocation.getArgument(1);
              assertTrue(metadataBeingSaved.isDeleted());
              assertNotNull(metadataBeingSaved.getDeletedOn());
              return null;
            })
        .when(rqueueRedisTemplate)
        .set(eq(MessageUtils.getMessageMetaId(id)), any(), eq(Duration.ofDays(7)));
    rqueueMessageMetadataService.deleteMessage(id, Duration.ofDays(7));
  }
}
