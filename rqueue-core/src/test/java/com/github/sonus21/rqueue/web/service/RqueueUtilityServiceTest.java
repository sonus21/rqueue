/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.web.service;

import static com.github.sonus21.rqueue.utils.TestUtils.createQueueConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueInternalPubSubChannel;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.web.service.impl.RqueueUtilityServiceImpl;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueUtilityServiceTest extends TestBase {

  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock
  private RqueueWebConfig rqueueWebConfig;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueMessageMetadataService messageMetadataService;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueStringDao rqueueStringDao;
  @Mock
  private RqueueInternalPubSubChannel rqueueInternalPubSubChannel;
  private RqueueUtilityService rqueueUtilityService;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueUtilityService =
        new RqueueUtilityServiceImpl(
            rqueueConfig,
            rqueueWebConfig,
            rqueueStringDao,
            rqueueSystemConfigDao,
            rqueueMessageTemplate,
            messageMetadataService,
            rqueueInternalPubSubChannel);
  }

  @Test
  void deleteMessage() {
    String id = UUID.randomUUID().toString();
    BaseResponse response = rqueueUtilityService.deleteMessage("notification", id);
    assertEquals(1, response.getCode());
    assertEquals("Queue config not found!", response.getMessage());

    QueueConfig queueConfig = createQueueConfig("notification", 3, 10000L, null);
    doReturn(queueConfig).when(rqueueSystemConfigDao).getConfigByName("notification", true);
    response = rqueueUtilityService.deleteMessage("notification", id);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    verify(messageMetadataService, times(1)).deleteMessage("notification", id, Duration.ofDays(30));
  }

  @Test
  void moveMessageInvalidRequest() {
    MessageMoveRequest request = new MessageMoveRequest();
    request.setSrc("job");
    request.setDst("__rq::d-queue::job");
    request.setSrcType(DataType.LIST);
    MessageMoveResponse response = rqueueUtilityService.moveMessage(request);
    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
  }

  @Test
  void moveMessageToZset() {
    doReturn(100).when(rqueueWebConfig).getMaxMessageMoveCount();
    MessageMoveRequest request = new MessageMoveRequest();
    request.setSrc("job");
    request.setDst("__rq::d-queue::job");
    request.setSrcType(DataType.LIST);
    request.setDstType(DataType.ZSET);
    Map<String, Serializable> other = new HashMap<>();
    request.setOthers(other);
    MessageMoveResult messageMoveResult = new MessageMoveResult(0, false);
    doReturn(messageMoveResult)
        .when(rqueueMessageTemplate)
        .moveMessageListToZset("job", "__rq::d-queue::job", 100, 0L);
    MessageMoveResponse response = rqueueUtilityService.moveMessage(request);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    assertFalse(response.isValue());

    other.put("newScore", "10000000");
    messageMoveResult = new MessageMoveResult(100, true);
    doReturn(messageMoveResult)
        .when(rqueueMessageTemplate)
        .moveMessageListToZset("job", "__rq::d-queue::job", 100, 10000000L);
    response = rqueueUtilityService.moveMessage(request);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    assertTrue(response.isValue());
    assertEquals(100, response.getNumberOfMessageTransferred());

    request.setSrcType(DataType.ZSET);
    doReturn(new MessageMoveResult(100, true))
        .when(rqueueMessageTemplate)
        .moveMessageZsetToZset("job", "__rq::d-queue::job", 100, 10000000L, false);
    response = rqueueUtilityService.moveMessage(request);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    assertTrue(response.isValue());
    assertEquals(100, response.getNumberOfMessageTransferred());

    other.put("fixedScore", true);
    other.put("newScore", "1000000000000000");

    doReturn(new MessageMoveResult(100, true))
        .when(rqueueMessageTemplate)
        .moveMessageZsetToZset("job", "__rq::d-queue::job", 100, 1000000000000000L, true);
    response = rqueueUtilityService.moveMessage(request);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    assertTrue(response.isValue());
    assertEquals(100, response.getNumberOfMessageTransferred());
  }

  @Test
  void moveMessageToList() {
    doReturn(100).when(rqueueWebConfig).getMaxMessageMoveCount();
    MessageMoveRequest request = new MessageMoveRequest();
    request.setSrc("__rq::d-queue:job");
    request.setSrcType(DataType.ZSET);
    request.setDst("job");
    request.setDstType(DataType.LIST);
    doReturn(new MessageMoveResult(100, true))
        .when(rqueueMessageTemplate)
        .moveMessageZsetToList("__rq::d-queue:job", "job", 100);
    MessageMoveResponse response = rqueueUtilityService.moveMessage(request);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    assertTrue(response.isValue());
    assertEquals(100, response.getNumberOfMessageTransferred());

    request.setSrcType(DataType.LIST);
    request.setSrc("job-dlq");
    request.setOthers(Collections.singletonMap("maxMessages", 200));

    doReturn(new MessageMoveResult(150, true))
        .when(rqueueMessageTemplate)
        .moveMessageListToList("job-dlq", "job", 200);
    response = rqueueUtilityService.moveMessage(request);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    assertTrue(response.isValue());
    assertEquals(150, response.getNumberOfMessageTransferred());
  }

  @Test
  void makeEmpty() {
    doReturn(org.springframework.data.redis.connection.DataType.STRING)
        .when(rqueueStringDao)
        .type("__rq::xqueue::{{job}}");
    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueUtilityService.makeEmpty("job", "__rq::xqueue::{{job}}"));

    doReturn(org.springframework.data.redis.connection.DataType.LIST)
        .when(rqueueStringDao)
        .type("__rq::queue::{{job}}");

    BooleanResponse response = rqueueUtilityService.makeEmpty("job", "__rq::queue::{{job}}");
    assertTrue(response.isValue());

    doReturn(org.springframework.data.redis.connection.DataType.ZSET)
        .when(rqueueStringDao)
        .type("__rq::d-queue::{{job}}");
    response = rqueueUtilityService.makeEmpty("job", "__rq::d-queue::{{job}}");
    assertTrue(response.isValue());
  }

  @Test
  void getDataType() {
    doReturn(org.springframework.data.redis.connection.DataType.STRING)
        .when(rqueueStringDao)
        .type("job");
    StringResponse stringResponse = new StringResponse("KEY");
    assertEquals(stringResponse, rqueueUtilityService.getDataType("job"));
  }
}
