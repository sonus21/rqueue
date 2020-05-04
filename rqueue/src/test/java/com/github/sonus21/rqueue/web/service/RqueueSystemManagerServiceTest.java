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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.web.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.web.service.impl.RqueueSystemManagerServiceImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

public class RqueueSystemManagerServiceTest {
  private RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueSystemConfigDao rqueueSystemConfigDao = mock(RqueueSystemConfigDao.class);
  private RqueueSystemManagerServiceImpl rqueueSystemManagerService =
      new RqueueSystemManagerServiceImpl(stringRqueueRedisTemplate, rqueueSystemConfigDao);
  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private QueueDetail slowQueueDetail = new QueueDetail(slowQueue, 3, "", true, 900000L);
  private QueueDetail fastQueueDetail = new QueueDetail(fastQueue, 3, "fast-dlq", false, 200000L);
  private QueueConfig slowQueueConfig =
      new QueueConfig(
          QueueUtils.getQueueConfigKey(slowQueue),
          slowQueue,
          slowQueueDetail.getNumRetries(),
          slowQueueDetail.isDelayedQueue(),
          slowQueueDetail.getVisibilityTimeout());
  private QueueConfig fastQueueConfig =
      new QueueConfig(
          QueueUtils.getQueueConfigKey(fastQueue),
          fastQueue,
          fastQueueDetail.getNumRetries(),
          fastQueueDetail.isDelayedQueue(),
          fastQueueDetail.getVisibilityTimeout(),
          fastQueueDetail.getDeadLetterQueueName());
  private Set<String> queues;

  @Before
  public void init() {
    queues = new HashSet<>();
    queues.add(slowQueue);
    queues.add(fastQueue);
  }

  @Test
  public void deleteQueue() {
    BaseResponse baseResponse = rqueueSystemManagerService.deleteQueue("test");
    assertEquals(1, baseResponse.getCode());
    assertEquals("Queue not found", baseResponse.getMessage());
    // String id, String name, int numRetry, boolean delayed, long visibilityTimeout
    QueueConfig queueConfig =
        new QueueConfig(QueueUtils.getQueueConfigKey("test"), "test", 10, true, 10000L);
    assertFalse(queueConfig.isDeleted());
    doReturn(queueConfig)
        .when(rqueueSystemConfigDao)
        .getQConfig(QueueUtils.getQueueConfigKey("test"));
    doReturn(redisTemplate).when(stringRqueueRedisTemplate).getRedisTemplate();
    baseResponse = rqueueSystemManagerService.deleteQueue("test");
    assertEquals(0, baseResponse.getCode());
    assertEquals("Queue deleted", baseResponse.getMessage());
    assertTrue(queueConfig.isDeleted());
    assertNotNull(queueConfig.getDeletedOn());
  }

  @Test
  public void getQueues() {
    doReturn(null).when(stringRqueueRedisTemplate).getMembers(QueueUtils.getQueuesKey());
    assertEquals(Collections.emptyList(), rqueueSystemManagerService.getQueues());
    doReturn(Collections.singleton("job"))
        .when(stringRqueueRedisTemplate)
        .getMembers(QueueUtils.getQueuesKey());
    assertEquals(Collections.singletonList("job"), rqueueSystemManagerService.getQueues());
  }

  @Test
  public void getQueueConfigs() {
    doReturn(queues).when(stringRqueueRedisTemplate).getMembers(QueueUtils.getQueuesKey());
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(
            queues.stream().map(QueueUtils::getQueueConfigKey).collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(slowQueueConfig, fastQueueConfig),
        rqueueSystemManagerService.getQueueConfigs());
  }

  @Test
  public void testGetQueueConfigs() {
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(
            queues.stream().map(QueueUtils::getQueueConfigKey).collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(slowQueueConfig, fastQueueConfig),
        rqueueSystemManagerService.getQueueConfigs(queues));
  }

  @Test
  public void getQueueConfig() {
    doReturn(Collections.singletonList(slowQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(Collections.singletonList(QueueUtils.getQueueConfigKey(slowQueue)));
    assertEquals(slowQueueConfig, rqueueSystemManagerService.getQueueConfig(slowQueue));
  }
}
