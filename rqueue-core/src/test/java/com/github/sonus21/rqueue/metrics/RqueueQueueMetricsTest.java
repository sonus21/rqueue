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

package com.github.sonus21.rqueue.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueQueueMetricsTest extends TestBase {

  private static final String queueName = "test";
  private static final String priorityName = "high";
  private static final QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
  private static final QueueDetail queueDetail2 =
      TestUtils.createQueueDetail(PriorityUtils.getQueueNameForPriority(queueName, priorityName));
  private final RqueueRedisTemplate<String> redisTemplate = mock(RqueueRedisTemplate.class);
  private final RqueueQueueMetrics queueMetrics = new RqueueQueueMetrics(redisTemplate);

  @BeforeAll
  static void setUp() {
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
    EndpointRegistry.register(queueDetail2);
  }

  @AfterAll
  static void tearDown() {
    EndpointRegistry.delete();
  }

  @Test
  void getPendingMessageCount() {
    doReturn(100L).when(redisTemplate).getListSize(queueDetail.getQueueName());
    assertEquals(100L, queueMetrics.getPendingMessageCount(queueName));
    assertEquals(-1L, queueMetrics.getPendingMessageCount("unknown"));
  }

  @Test
  void getPendingMessageCountWithPriority() {
    doReturn(100L).when(redisTemplate).getListSize(queueDetail2.getQueueName());
    assertEquals(100L, queueMetrics.getPendingMessageCount(queueName, priorityName));
    assertEquals(-1L, queueMetrics.getPendingMessageCount("unknown", priorityName));
  }

  @Test
  void getScheduledMessageCount() {
    doReturn(100L).when(redisTemplate).getZsetSize(queueDetail.getScheduledQueueName());
    assertEquals(100L, queueMetrics.getScheduledMessageCount(queueName));
    assertEquals(-1L, queueMetrics.getScheduledMessageCount("unknown"));
  }

  @Test
  void getScheduledMessageCountWithPriority() {
    doReturn(100L).when(redisTemplate).getZsetSize(queueDetail2.getScheduledQueueName());
    assertEquals(100L, queueMetrics.getScheduledMessageCount(queueName, priorityName));
    assertEquals(-1L, queueMetrics.getScheduledMessageCount("unknown", priorityName));
  }

  @Test
  void getProcessingMessageCount() {
    doReturn(100L).when(redisTemplate).getZsetSize(queueDetail.getProcessingQueueName());
    assertEquals(100L, queueMetrics.getProcessingMessageCount(queueName));
    assertEquals(-1L, queueMetrics.getProcessingMessageCount("unknown"));
  }

  @Test
  void getProcessingMessageCountWithPriority() {
    doReturn(100L).when(redisTemplate).getZsetSize(queueDetail2.getProcessingQueueName());
    assertEquals(100L, queueMetrics.getProcessingMessageCount(queueName, priorityName));
    assertEquals(-1L, queueMetrics.getProcessingMessageCount("unknown", priorityName));
  }
}
