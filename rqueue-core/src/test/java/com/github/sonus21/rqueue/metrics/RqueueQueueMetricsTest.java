/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
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
import com.github.sonus21.rqueue.utils.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class RqueueQueueMetricsTest extends TestBase {
  private static final QueueDetail queueDetail = TestUtils.createQueueDetail("test");
  private final RqueueRedisTemplate<String> redisTemplate = mock(RqueueRedisTemplate.class);
  private final RqueueQueueMetrics queueMetrics = new RqueueQueueMetrics(redisTemplate);

  @BeforeAll
  static void setUp() {
    EndpointRegistry.register(queueDetail);
  }

  @AfterAll
  static void tearDown() {
    EndpointRegistry.delete();
  }

  @Test
  void getPendingMessageCount() {
    doReturn(100L).when(redisTemplate).getListSize(queueDetail.getQueueName());
    assertEquals(100L, queueMetrics.getPendingMessageCount(queueDetail.getName()));
    assertEquals(-1L, queueMetrics.getPendingMessageCount("unknown"));
  }

  @Test
  void getScheduledMessageCount() {
    doReturn(100L).when(redisTemplate).getZsetSize(queueDetail.getScheduledQueueName());
    assertEquals(100L, queueMetrics.getScheduledMessageCount(queueDetail.getName()));
    assertEquals(-1L, queueMetrics.getScheduledMessageCount("unknown"));
  }

  @Test
  void getProcessingMessageCount() {
    doReturn(100L).when(redisTemplate).getZsetSize(queueDetail.getProcessingQueueName());
    assertEquals(100L, queueMetrics.getProcessingMessageCount(queueDetail.getName()));
    assertEquals(-1L, queueMetrics.getProcessingMessageCount("unknown"));
  }
}
