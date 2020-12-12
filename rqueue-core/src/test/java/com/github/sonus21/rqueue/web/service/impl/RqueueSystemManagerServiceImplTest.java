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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class RqueueSystemManagerServiceImplTest {
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueSystemConfigDao rqueueSystemConfigDao = mock(RqueueSystemConfigDao.class);
  private RqueueConfig rqueueConfig = mock(RqueueConfig.class);
  private RqueueSystemManagerServiceImpl rqueueSystemManagerService =
      new RqueueSystemManagerServiceImpl(
          rqueueConfig, stringRqueueRedisTemplate, rqueueSystemConfigDao);
  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private String normalQueue = "normal-queue";
  private QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private QueueDetail fastQueueDetail =
      TestUtils.createQueueDetail(fastQueue, 3, 200000L, "fast-dlq");
  private QueueDetail normalQueueDetail =
      TestUtils.createQueueDetail(normalQueue, 3, 100000L, "normal-dlq");
  private QueueConfig slowQueueConfig = slowQueueDetail.toConfig();
  private QueueConfig fastQueueConfig = fastQueueDetail.toConfig();

  @BeforeEach
  public void init() {
    EndpointRegistry.delete();
    slowQueueConfig.setId(TestUtils.getQueueConfigKey(slowQueue));
    fastQueueConfig.setId(TestUtils.getQueueConfigKey(fastQueue));
    EndpointRegistry.register(slowQueueDetail);
    EndpointRegistry.register(fastQueueDetail);
  }

  @Test
  public void onApplicationEventStop() {
    RqueueBootstrapEvent event = new RqueueBootstrapEvent("Container", false);
    rqueueSystemManagerService.onApplicationEvent(event);
    verifyNoInteractions(stringRqueueRedisTemplate);
    verifyNoInteractions(rqueueSystemConfigDao);
  }

  @Test
  public void onApplicationEventStartEmpty() {
    EndpointRegistry.delete();
    RqueueBootstrapEvent event = new RqueueBootstrapEvent("Container", true);
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
    assertEquals(expectedConfig.getNumRetry(), queueConfig.getNumRetry());
    assertEquals(expectedConfig.getVisibilityTimeout(), queueConfig.getVisibilityTimeout());
    assertEquals(expectedConfig.getDeadLetterQueues(), queueConfig.getDeadLetterQueues());
  }

  @Test
  public void onApplicationEventStartCreateAllQueueConfigs() {
    doReturn("__rq::queues").when(rqueueConfig).getQueuesKey();
    doAnswer(
            invocation -> {
              String name = invocation.getArgument(0);
              return "__rq::q-config::" + name;
            })
        .when(rqueueConfig)
        .getQueueConfigKey(anyString());
    RqueueBootstrapEvent event = new RqueueBootstrapEvent("Container", true);
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
        .addToSet(eq(TestUtils.getQueuesKey()), any());
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
    RqueueBootstrapEvent event = new RqueueBootstrapEvent("Container", true);
    EndpointRegistry.register(normalQueueDetail);
    doAnswer(
            invocation -> {
              String name = invocation.getArgument(0);
              return "__rq::q-config::" + name;
            })
        .when(rqueueConfig)
        .getQueueConfigKey(anyString());
    QueueConfig fastQueueConfig =
        TestUtils.createQueueConfig(
            fastQueue, fastQueueDetail.getNumRetry(), fastQueueDetail.getVisibilityTimeout(), null);
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(anyCollection());

    QueueConfig expectedFastQueueConfig =
        TestUtils.createQueueConfig(
            fastQueue,
            fastQueueDetail.getNumRetry(),
            fastQueueDetail.getVisibilityTimeout(),
            fastQueueDetail.getDeadLetterQueueName());
    QueueConfig normalQueueConfig =
        TestUtils.createQueueConfig(
            normalQueue,
            normalQueueDetail.getNumRetry(),
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
