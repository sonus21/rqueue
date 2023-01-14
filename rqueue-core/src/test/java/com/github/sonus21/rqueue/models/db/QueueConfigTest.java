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

package com.github.sonus21.rqueue.models.db;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class QueueConfigTest extends TestBase {

  @Test
  void builderToString() {
    QueueConfig.QueueConfigBuilder builder = QueueConfig.builder().concurrency(new MinMax<>(1, 5));
    System.out.println(builder.toString());
  }

  @Test
  void addDeadLetterQueue() {
    QueueConfig queueConfig = new QueueConfig();
    List<DeadLetterQueue> queues = new LinkedList<>();
    queues.add(new DeadLetterQueue("test-dlq", false));
    assertTrue(queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq", false)));
    assertEquals(queues, queueConfig.getDeadLetterQueues());
    assertFalse(queueConfig.addDeadLetterQueue(new DeadLetterQueue("test-dlq", false)));
    assertEquals(queues, queueConfig.getDeadLetterQueues());
    assertTrue(queueConfig.addDeadLetterQueue(new DeadLetterQueue("test2-dlq", false)));
    queues.add(new DeadLetterQueue("test2-dlq", false));
    assertEquals(queues, queueConfig.getDeadLetterQueues());
  }

  @Test
  void visibilityTimeout() {
    QueueConfig queueConfig = new QueueConfig();
    assertTrue(queueConfig.updateVisibilityTimeout(100L));
    assertEquals(100L, queueConfig.getVisibilityTimeout());
    assertFalse(queueConfig.updateVisibilityTimeout(100L));
    assertEquals(100L, queueConfig.getVisibilityTimeout());
  }

  @Test
  void builder() {
    QueueConfig queueConfig =
        QueueConfig.builder().id("__rq::q").name("q").visibilityTimeout(100L).numRetry(100).build();
    assertEquals("__rq::q", queueConfig.getId());
    assertEquals("q", queueConfig.getName());
    assertEquals(100L, queueConfig.getVisibilityTimeout());
    assertEquals(100, queueConfig.getNumRetry());
    assertNull(queueConfig.getDeadLetterQueues());
    assertNull(queueConfig.getUpdatedOn());
    assertNull(queueConfig.getCreatedOn());
  }

  @Test
  void updateTime() {
    TestUtils.createQueueConfig("test");
  }

  @Test
  void updateRetryCount() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertTrue(queueConfig.updateRetryCount(100));
    assertEquals(100, queueConfig.getNumRetry());
    assertFalse(queueConfig.updateRetryCount(100));
  }

  @Test
  void updateVisibilityTimeout() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertTrue(queueConfig.updateVisibilityTimeout(1000L));
    assertEquals(1000L, queueConfig.getVisibilityTimeout());
    assertFalse(queueConfig.updateVisibilityTimeout(1000L));
  }

  @Test
  void updateConcurrency() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    MinMax<Integer> concurrency = new MinMax<>(1, 10);
    assertTrue(queueConfig.updateConcurrency(concurrency));
    assertEquals(concurrency, queueConfig.getConcurrency());
    assertFalse(queueConfig.updateConcurrency(concurrency));
  }

  @Test
  void updatePriority() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    Map<String, Integer> priorityMap = new HashMap<>();
    priorityMap.put(Constants.DEFAULT_PRIORITY_KEY, 100);
    priorityMap.put("critical", 100);
    priorityMap.put("high", 50);

    assertTrue(queueConfig.updatePriority(priorityMap));
    assertEquals(priorityMap, queueConfig.getPriority());

    assertFalse(queueConfig.updatePriority(priorityMap));

    priorityMap = new HashMap<>();
    priorityMap.put("critical", 100);
    priorityMap.put("high", 20);
    priorityMap.put("low", 50);
    assertTrue(queueConfig.updatePriority(priorityMap));
    assertEquals(priorityMap, queueConfig.getPriority());
    assertFalse(queueConfig.updatePriority(priorityMap));
  }

  @Test
  void isDeadLetterQueue() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test", "test-dlq");
    assertTrue(queueConfig.isDeadLetterQueue("test-dlq"));
    assertFalse(queueConfig.isDeadLetterQueue("test"));
  }

  @Test
  void hasDeadLetterQueue() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertFalse(queueConfig.hasDeadLetterQueue());
    queueConfig = TestUtils.createQueueConfig("test", "test-dlq");
    assertTrue(queueConfig.hasDeadLetterQueue());
  }

  @Test
  void updatePriorityGroup() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertTrue(queueConfig.updatePriorityGroup("new-group"));
    assertEquals("new-group", queueConfig.getPriorityGroup());
    assertFalse(queueConfig.updatePriorityGroup("new-group"));
  }

  @Test
  void equals() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    QueueConfig queueConfig2 = TestUtils.createQueueConfig("test");
    assertEquals(queueConfig, queueConfig2);
  }
}
