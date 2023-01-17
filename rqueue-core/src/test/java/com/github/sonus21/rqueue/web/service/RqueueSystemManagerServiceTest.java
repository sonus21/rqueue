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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.impl.RqueueSystemManagerServiceImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueSystemManagerServiceTest extends TestBase {

  private final String slowQueue = "slow-queue";
  private final String fastQueue = "fast-queue";
  private final QueueDetail fastQueueDetail =
      TestUtils.createQueueDetail(fastQueue, 200000L, "fast-dlq");
  private final QueueConfig fastQueueConfig = fastQueueDetail.toConfig();
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue, 900000L);
  private final QueueConfig slowQueueConfig = slowQueueDetail.toConfig();
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueStringDao rqueueStringDao;
  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  private RqueueSystemManagerService rqueueSystemManagerService;
  private Set<String> queues;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    rqueueSystemManagerService =
        new RqueueSystemManagerServiceImpl(
            rqueueConfig, rqueueStringDao, rqueueSystemConfigDao, rqueueMessageMetadataService);
    queues = new HashSet<>();
    queues.add(slowQueue);
    queues.add(fastQueue);
  }

  @Test
  void deleteQueue() {
    BaseResponse baseResponse = rqueueSystemManagerService.deleteQueue("test");
    assertEquals(1, baseResponse.getCode());
    assertEquals("Queue not found", baseResponse.getMessage());
    QueueConfig queueConfig = TestUtils.createQueueConfig("test", 10, 10000L, null);
    assertFalse(queueConfig.isDeleted());
    doReturn(queueConfig).when(rqueueSystemConfigDao).getConfigByName("test", true);
    baseResponse = rqueueSystemManagerService.deleteQueue("test");
    assertEquals(0, baseResponse.getCode());
    assertEquals("Queue deleted", baseResponse.getMessage());
    assertTrue(queueConfig.isDeleted());
    assertNotNull(queueConfig.getDeletedOn());
  }

  @Test
  void getQueues() {
    doReturn("__rq::queues").when(rqueueConfig).getQueuesKey();
    doReturn(Collections.emptyList()).when(rqueueStringDao).readFromSet(TestUtils.getQueuesKey());
    assertEquals(Collections.emptyList(), rqueueSystemManagerService.getQueues());
    doReturn(Collections.singletonList("job"))
        .when(rqueueStringDao)
        .readFromSet(TestUtils.getQueuesKey());
    assertEquals(Collections.singletonList("job"), rqueueSystemManagerService.getQueues());
  }

  @Test
  void getQueueConfigs() {
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-config::" + name;
        })
        .when(rqueueConfig)
        .getQueueConfigKey(anyString());
    doReturn("__rq::queues").when(rqueueConfig).getQueuesKey();
    doReturn(new ArrayList<>(queues)).when(rqueueStringDao).readFromSet(TestUtils.getQueuesKey());
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(
            queues.stream().map(TestUtils::getQueueConfigKey).collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(slowQueueConfig, fastQueueConfig),
        rqueueSystemManagerService.getQueueConfigs());
  }

  @Test
  void getSortedQueueConfigs() {
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-config::" + name;
        })
        .when(rqueueConfig)
        .getQueueConfigKey(anyString());
    doReturn("__rq::queues").when(rqueueConfig).getQueuesKey();
    doReturn(new ArrayList<>(queues)).when(rqueueStringDao).readFromSet(TestUtils.getQueuesKey());
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(
            queues.stream()
                .map(TestUtils::getQueueConfigKey)
                .sorted()
                .collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(fastQueueConfig, slowQueueConfig),
        rqueueSystemManagerService.getSortedQueueConfigs());
  }

  @Test
  void getQueueConfig() {
    doAnswer(
        invocation -> {
          String name = invocation.getArgument(0);
          return "__rq::q-config::" + name;
        })
        .when(rqueueConfig)
        .getQueueConfigKey(anyString());
    doReturn(Collections.singletonList(slowQueueConfig))
        .when(rqueueSystemConfigDao)
        .findAllQConfig(Collections.singletonList(TestUtils.getQueueConfigKey(slowQueue)));
    assertEquals(slowQueueConfig, rqueueSystemManagerService.getQueueConfig(slowQueue));
  }
}
