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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.doAnswer;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.dao.RqueueQStore;
import com.github.sonus21.rqueue.web.service.impl.RqueueSystemManagerServiceImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.redis.core.RedisTemplate;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueSystemManagerServiceTest {
  private RqueueConfig rqueueConfig = mock(RqueueConfig.class);
  private RedisTemplate<String, String> redisTemplate = mock(RedisTemplate.class);
  private RqueueRedisTemplate<String> stringRqueueRedisTemplate = mock(RqueueRedisTemplate.class);
  private RqueueQStore rqueueQStore = mock(RqueueQStore.class);
  private RqueueSystemManagerService rqueueSystemManagerService =
      new RqueueSystemManagerServiceImpl(
          rqueueConfig, stringRqueueRedisTemplate, rqueueQStore);
  private String slowQueue = "slow-queue";
  private String fastQueue = "fast-queue";
  private QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue, 900000L);
  private QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue, 200000L, "fast-dlq");
  private QueueConfig slowQueueConfig = slowQueueDetail.toConfig();
  private QueueConfig fastQueueConfig = fastQueueDetail.toConfig();
  private Set<String> queues;

  @Before
  public void init() {
    queues = new HashSet<>();
    queues.add(slowQueue);
    queues.add(fastQueue);
    doReturn("__rq::queues").when(rqueueConfig).getQueuesKey();
    doAnswer(
            invocation -> {
              String name = invocation.getArgument(0);
              return "__rq::q-config::" + name;
            })
        .when(rqueueConfig)
        .getQueueConfigKey(anyString());
  }

  @Test
  public void deleteQueue() {
    BaseResponse baseResponse = rqueueSystemManagerService.deleteQueue("test");
    assertEquals(1, baseResponse.getCode());
    assertEquals("Queue not found", baseResponse.getMessage());
    QueueConfig queueConfig = TestUtils.createQueueConfig("test", 10, 10000L, null);
    assertFalse(queueConfig.isDeleted());
    doReturn(queueConfig)
        .when(rqueueQStore)
        .getQConfig(TestUtils.getQueueConfigKey("test"));
    doReturn(redisTemplate).when(stringRqueueRedisTemplate).getRedisTemplate();
    baseResponse = rqueueSystemManagerService.deleteQueue("test");
    assertEquals(0, baseResponse.getCode());
    assertEquals("Queue deleted", baseResponse.getMessage());
    assertTrue(queueConfig.isDeleted());
    assertNotNull(queueConfig.getDeletedOn());
  }

  @Test
  public void getQueues() {
    doReturn(null).when(stringRqueueRedisTemplate).getMembers(TestUtils.getQueuesKey());
    assertEquals(Collections.emptyList(), rqueueSystemManagerService.getQueues());
    doReturn(Collections.singleton("job"))
        .when(stringRqueueRedisTemplate)
        .getMembers(TestUtils.getQueuesKey());
    assertEquals(Collections.singletonList("job"), rqueueSystemManagerService.getQueues());
  }

  @Test
  public void getQueueConfigs() {
    doReturn(queues).when(stringRqueueRedisTemplate).getMembers(TestUtils.getQueuesKey());
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueQStore)
        .findAllQConfig(
            queues.stream().map(TestUtils::getQueueConfigKey).collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(slowQueueConfig, fastQueueConfig),
        rqueueSystemManagerService.getQueueConfigs());
  }

  @Test
  public void getSortedQueueConfigs() {
    doReturn(queues).when(stringRqueueRedisTemplate).getMembers(TestUtils.getQueuesKey());
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueQStore)
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
  public void testGetQueueConfigs() {
    doReturn(Arrays.asList(slowQueueConfig, fastQueueConfig))
        .when(rqueueQStore)
        .findAllQConfig(
            queues.stream().map(TestUtils::getQueueConfigKey).collect(Collectors.toList()));
    assertEquals(
        Arrays.asList(slowQueueConfig, fastQueueConfig),
        rqueueSystemManagerService.getQueueConfigs(queues));
  }

  @Test
  public void getQueueConfig() {
    doReturn(Collections.singletonList(slowQueueConfig))
        .when(rqueueQStore)
        .findAllQConfig(Collections.singletonList(TestUtils.getQueueConfigKey(slowQueue)));
    assertEquals(slowQueueConfig, rqueueSystemManagerService.getQueueConfig(slowQueue));
  }
}
