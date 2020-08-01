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

import static com.github.sonus21.rqueue.utils.TestUtils.createQueueConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueUtilityServiceTest {
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueQStore rqueueQStore = mock(RqueueQStore.class);
  private RqueueWebConfig rqueueWebConfig = mock(RqueueWebConfig.class);
  private RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
  private RqueueMessageMetadataService messageMetadataService =
      mock(RqueueMessageMetadataService.class);
  private RqueueConfig rqueueConfig = mock(RqueueConfig.class);
  private RqueueUtilityService rqueueUtilityService =
      new RqueueUtilityServiceImpl(
          rqueueConfig,
          rqueueWebConfig,
          stringRqueueRedisTemplate,
          rqueueQStore,
          rqueueMessageTemplate,
          messageMetadataService);

  @Test
  public void deleteMessage() {
    doReturn("__rq::q-config::notification").when(rqueueConfig).getQueueConfigKey("notification");
    String id = UUID.randomUUID().toString();
    BaseResponse response = rqueueUtilityService.deleteMessage("notification", id);
    assertEquals(1, response.getCode());
    assertEquals("Queue config not found!", response.getMessage());

    QueueConfig queueConfig = createQueueConfig("notification", 3, 10000L, null);
    doReturn(queueConfig).when(rqueueQStore).getQConfig(queueConfig.getId());
    response = rqueueUtilityService.deleteMessage("notification", id);
    assertEquals(0, response.getCode());
    assertNull(response.getMessage());
    verify(messageMetadataService, times(1)).deleteMessage(id, Duration.ofDays(30));
  }

  @Test
  public void moveMessageInvalidRequest() {
    MessageMoveRequest request = new MessageMoveRequest();
    request.setSrc("job");
    request.setDst("__rq::d-queue::job");
    request.setSrcType(DataType.LIST);
    MessageMoveResponse response = rqueueUtilityService.moveMessage(request);
    assertEquals(1, response.getCode());
    assertNotNull(response.getMessage());
  }

  @Test
  public void moveMessageToZset() {
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
  public void moveMessageToList() {
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
  public void deleteQueueMessages() {
    doReturn(org.springframework.data.redis.connection.DataType.LIST)
        .when(stringRqueueRedisTemplate)
        .type("job");
    BooleanResponse response = rqueueUtilityService.deleteQueueMessages("job", 0);
    assertTrue(response.isValue());
    response = rqueueUtilityService.deleteQueueMessages("job", 100);
    assertTrue(response.isValue());

    doReturn(org.springframework.data.redis.connection.DataType.ZSET)
        .when(stringRqueueRedisTemplate)
        .type("job");
    response = rqueueUtilityService.deleteQueueMessages("job", 100);
    assertFalse(response.isValue());
    verify(stringRqueueRedisTemplate, times(1)).ltrim("job", 2, 1);
    verify(stringRqueueRedisTemplate, times(1)).ltrim("job", -100, -1);
  }

  @Test
  public void getLatestVersion() {}

  @Test
  public void getDataType() {
    doReturn(org.springframework.data.redis.connection.DataType.STRING)
        .when(stringRqueueRedisTemplate)
        .type("job");
    StringResponse stringResponse = new StringResponse("KEY");
    assertEquals(stringResponse, rqueueUtilityService.getDataType("job"));
  }
}
