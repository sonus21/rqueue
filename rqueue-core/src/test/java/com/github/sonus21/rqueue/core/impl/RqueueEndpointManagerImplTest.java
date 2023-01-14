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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueEndpointManagerImplTest extends TestBase {

  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final MessageHeaders messageHeaders = RqueueMessageHeaders.emptyMessageHeaders();
  private final String queue = "test-queue";
  private final String[] priorities = new String[]{"high", "medium", "low"};

  @Mock
  private RqueueMessageTemplate messageTemplate;
  @Mock
  private RedisConnectionFactory redisConnectionFactory;
  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock
  private RqueueUtilityService rqueueUtilityService;
  private RqueueEndpointManager rqueueEndpointManager;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueEndpointManager =
        new RqueueEndpointManagerImpl(messageTemplate, messageConverter, messageHeaders);
    RqueueConfig rqueueConfig = new RqueueConfig(redisConnectionFactory, null, false, 1);
    FieldUtils.writeField(rqueueEndpointManager, "rqueueConfig", rqueueConfig, true);
    FieldUtils.writeField(
        rqueueEndpointManager, "rqueueUtilityService", rqueueUtilityService, true);
    FieldUtils.writeField(
        rqueueEndpointManager, "rqueueSystemConfigDao", rqueueSystemConfigDao, true);
    EndpointRegistry.delete();
  }

  @AfterEach
  public void clean() {
    EndpointRegistry.delete();
  }

  @Test
  void isQueueRegistered() {
    assertFalse(rqueueEndpointManager.isQueueRegistered(queue));
    rqueueEndpointManager.registerQueue(queue);
    assertTrue(rqueueEndpointManager.isQueueRegistered(queue));
  }

  @Test
  void getQueueConfig() {
    rqueueEndpointManager.registerQueue(queue, priorities);
    assertEquals(4, rqueueEndpointManager.getQueueConfig(queue).size());
  }

  @Test
  void isQueuePaused() {
    assertThrows(IllegalArgumentException.class, () -> rqueueEndpointManager.isQueuePaused(null));
    // no config
    assertThrows(IllegalStateException.class, () -> rqueueEndpointManager.isQueuePaused(queue));

    // config with true
    QueueConfig queueConfig = TestUtils.createQueueConfig(queue);
    queueConfig.setPaused(true);
    doReturn(queueConfig).when(rqueueSystemConfigDao).getConfigByName(queue, false);
    assertTrue(rqueueEndpointManager.isQueuePaused(queue));

    // config  with false
    queueConfig.setPaused(false);
    doReturn(queueConfig).when(rqueueSystemConfigDao).getConfigByName(queue, false);
    assertFalse(rqueueEndpointManager.isQueuePaused(queue));
  }

  @Test
  void isQueuePausedWithPriority() {
    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueEndpointManager.isQueuePaused(null, priorities[0]));
    assertThrows(
        IllegalArgumentException.class, () -> rqueueEndpointManager.isQueuePaused(queue, null));
    // no config
    assertThrows(
        IllegalStateException.class,
        () -> rqueueEndpointManager.isQueuePaused(queue, priorities[0]));

    // config with true
    QueueConfig queueConfig = TestUtils.createQueueConfig(queue);
    queueConfig.setPaused(true);
    doReturn(queueConfig)
        .when(rqueueSystemConfigDao)
        .getConfigByName(PriorityUtils.getQueueNameForPriority(queue, priorities[0]), false);
    assertTrue(rqueueEndpointManager.isQueuePaused(queue, priorities[0]));

    // config  with false
    queueConfig.setPaused(false);
    doReturn(queueConfig)
        .when(rqueueSystemConfigDao)
        .getConfigByName(PriorityUtils.getQueueNameForPriority(queue, priorities[0]), false);
    assertFalse(rqueueEndpointManager.isQueuePaused(queue, priorities[0]));
  }

  @Test
  void pauseQueue() {
    assertThrows(
        IllegalArgumentException.class, () -> rqueueEndpointManager.pauseUnpauseQueue(null, true));

    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest(true);
    request.setName(queue);
    doReturn(new BaseResponse()).when(rqueueUtilityService).pauseUnpauseQueue(request);
    assertTrue(rqueueEndpointManager.pauseUnpauseQueue(queue, true));

    request.setPause(false);
    doReturn(BaseResponse.builder().code(404).build())
        .when(rqueueUtilityService)
        .pauseUnpauseQueue(request);
    assertFalse(rqueueEndpointManager.pauseUnpauseQueue(queue, false));
  }

  @Test
  void pauseQueueWithPriority() {
    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueEndpointManager.pauseUnpauseQueue(null, priorities[0], true));

    assertThrows(
        IllegalArgumentException.class,
        () -> rqueueEndpointManager.pauseUnpauseQueue(queue, null, true));

    PauseUnpauseQueueRequest request = new PauseUnpauseQueueRequest(true);
    request.setName(PriorityUtils.getQueueNameForPriority(queue, priorities[0]));
    doReturn(new BaseResponse()).when(rqueueUtilityService).pauseUnpauseQueue(request);
    assertTrue(rqueueEndpointManager.pauseUnpauseQueue(queue, priorities[0], true));

    request.setPause(false);
    doReturn(BaseResponse.builder().code(404).build())
        .when(rqueueUtilityService)
        .pauseUnpauseQueue(request);
    assertFalse(rqueueEndpointManager.pauseUnpauseQueue(queue, priorities[0], false));
  }
}
