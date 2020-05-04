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

package com.github.sonus21.rqueue.web.service.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.utils.QueueUtils;
import com.github.sonus21.rqueue.web.dao.RqueueSystemConfigDao;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

public class RqueueSystemManagerServiceImplTest {
  private RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueSystemConfigDao rqueueSystemConfigDao = mock(RqueueSystemConfigDao.class);
  private RqueueSystemManagerServiceImpl rqueueSystemManagerService =
      new RqueueSystemManagerServiceImpl(stringRqueueRedisTemplate, rqueueSystemConfigDao);
  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private String normalQueue = "normal-queue";
  private QueueDetail slowQueueDetail = new QueueDetail(slowQueue, 3, "", true, 900000L);
  private QueueDetail fastQueueDetail = new QueueDetail(fastQueue, 3, "fast-dlq", false, 200000L);
  private QueueDetail normalQueueDetail =
      new QueueDetail(normalQueue, 3, "normal-dlq", false, 100000L);
  private Map<String, QueueDetail> queueNameToQueueDetail = new HashMap<>();
  private QueueConfig slowQueueConfig;
  private QueueConfig fastQueueConfig;

  @Before
  public void init() {
    queueNameToQueueDetail.put(slowQueue, slowQueueDetail);
    queueNameToQueueDetail.put(fastQueue, fastQueueDetail);
    slowQueueConfig =
        new QueueConfig(
            QueueUtils.getQueueConfigKey(slowQueue),
            slowQueue,
            slowQueueDetail.getNumRetries(),
            slowQueueDetail.isDelayedQueue(),
            slowQueueDetail.getVisibilityTimeout());
    fastQueueConfig =
        new QueueConfig(
            QueueUtils.getQueueConfigKey(fastQueue),
            fastQueue,
            fastQueueDetail.getNumRetries(),
            fastQueueDetail.isDelayedQueue(),
            fastQueueDetail.getVisibilityTimeout(),
            fastQueueDetail.getDeadLetterQueueName());
  }

  @Test
  public void onApplicationEventStop() {
    QueueInitializationEvent event =
        new QueueInitializationEvent("Container", queueNameToQueueDetail, false);
    rqueueSystemManagerService.onApplicationEvent(event);
    verifyNoInteractions(stringRqueueRedisTemplate);
    verifyNoInteractions(rqueueSystemConfigDao);
  }

  @Test
  public void onApplicationEventStartEmpty() {
    QueueInitializationEvent event =
        new QueueInitializationEvent("Container", Collections.emptyMap(), true);
    rqueueSystemManagerService.onApplicationEvent(event);
    verifyNoInteractions(stringRqueueRedisTemplate);
    verifyNoInteractions(rqueueSystemConfigDao);
  }

  public void verifyConfigData(QueueConfig expectedConfig, QueueConfig queueConfig) {
    assertFalse(queueConfig.isDeleted());
    assertNull(queueConfig.getDeletedOn());
    assertNotNull(queueConfig.getCreatedOn());
    assertNotNull(queueConfig.getUpdatedOn());
    assertEquals(expectedConfig.getId(), queueConfig.getId());
    assertEquals(expectedConfig.getName(), queueConfig.getName());
    assertEquals(expectedConfig.isDelayed(), queueConfig.isDelayed());
    assertEquals(expectedConfig.getNumRetry(), queueConfig.getNumRetry());
    assertEquals(expectedConfig.getVisibilityTimeout(), queueConfig.getVisibilityTimeout());
    assertEquals(expectedConfig.getDeadLetterQueues(), queueConfig.getDeadLetterQueues());
  }

  @Test
  public void onApplicationEventStartCreateAllQueueConfigs() {
    QueueInitializationEvent event =
        new QueueInitializationEvent("Container", queueNameToQueueDetail, true);
    doAnswer(
            invocation -> {
              if (slowQueue.equals(invocation.getArgument(1))) {
                assertEquals(fastQueue, invocation.getArgument(2));
              } else if (fastQueue.equals(invocation.getArgument(1))) {
                assertEquals(slowQueue, invocation.getArgument(2));
              } else {
                fail();
              }
              return 2L;
            })
        .when(stringRqueueRedisTemplate)
        .addToSet(eq(QueueUtils.getQueuesKey()), any());
    doAnswer(
            invocation -> {
              List<QueueConfig> queueConfigs = invocation.getArgument(0);
              assertEquals(2, queueConfigs.size());
              int slowId = 0, fastId = 1;
              if (queueConfigs.get(0).getName().equals(fastQueue)) {
                fastId = 0;
                slowId = 1;
              }
              QueueConfig fastQueueConfigToBeSaved = queueConfigs.get(fastId);
              QueueConfig slowQueueConfigToBeSaved = queueConfigs.get(slowId);
              verifyConfigData(fastQueueConfig, fastQueueConfigToBeSaved);
              verifyConfigData(slowQueueConfig, slowQueueConfigToBeSaved);
              return null;
            })
        .when(rqueueSystemConfigDao)
        .saveAllQConfig(anyList());
    rqueueSystemManagerService.onApplicationEvent(event);
  }

  @Test
  public void onApplicationEventStartCreateAndUpdateQueueConfigs() {
    Map<String, QueueDetail> queueDetailMap = new HashMap<>(queueNameToQueueDetail);
    queueDetailMap.put(normalQueue, normalQueueDetail);
    QueueInitializationEvent event =
        new QueueInitializationEvent("Container", queueDetailMap, true);
    QueueConfig fastQueueConfig =
        new QueueConfig(
            QueueUtils.getQueueConfigKey(fastQueue),
            fastQueue,
            fastQueueDetail.getNumRetries(),
            fastQueueDetail.isDelayedQueue(),
            fastQueueDetail.getVisibilityTimeout());

    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(anyCollection());

    QueueConfig expectedFastQueueConfig =
        new QueueConfig(
            QueueUtils.getQueueConfigKey(fastQueue),
            fastQueue,
            fastQueueDetail.getNumRetries(),
            fastQueueDetail.isDelayedQueue(),
            fastQueueDetail.getVisibilityTimeout(),
            fastQueueDetail.getDeadLetterQueueName());
    QueueConfig normalQueueConfig =
        new QueueConfig(
            QueueUtils.getQueueConfigKey(normalQueue),
            normalQueue,
            normalQueueDetail.getNumRetries(),
            normalQueueDetail.isDelayedQueue(),
            normalQueueDetail.getVisibilityTimeout(),
            normalQueueDetail.getDeadLetterQueueName());

    doAnswer(
            invocation -> {
              List<QueueConfig> queueConfigs = invocation.getArgument(0);
              assertEquals(2, queueConfigs.size());
              int normalId = 0, fastId = 1;
              if (queueConfigs.get(0).getName().equals(fastQueue)) {
                fastId = 0;
                normalId = 1;
              }
              QueueConfig fastQueueConfigToBeSaved = queueConfigs.get(fastId);
              QueueConfig normalQueueConfigToBeSaved = queueConfigs.get(normalId);
              verifyConfigData(expectedFastQueueConfig, fastQueueConfigToBeSaved);
              verifyConfigData(normalQueueConfig, normalQueueConfigToBeSaved);
              return null;
            })
        .when(rqueueSystemConfigDao)
        .saveAllQConfig(anyList());
    rqueueSystemManagerService.onApplicationEvent(event);
  }
}
