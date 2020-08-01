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

package com.github.sonus21.rqueue.models.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.models.MinMax;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class QueueConfigTest {

  @Test
  public void testBuilderToString() {
    QueueConfig.QueueConfigBuilder builder = QueueConfig.builder().concurrency(new MinMax<>(1, 5));
    System.out.println(builder.toString());
  }

  @Test
  public void addDeadLetterQueue() {
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
  public void testVisibilityTimeout() {
    QueueConfig queueConfig = new QueueConfig();
    assertTrue(queueConfig.updateVisibilityTimeout(100L));
    assertEquals(100L, queueConfig.getVisibilityTimeout());
    assertFalse(queueConfig.updateVisibilityTimeout(100L));
    assertEquals(100L, queueConfig.getVisibilityTimeout());
  }

  @Test
  public void testBuilder() {
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
  public void updateTime() {
    TestUtils.createQueueConfig("test");
  }

  @Test
  public void updateRetryCount() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertTrue(queueConfig.updateRetryCount(100));
    assertEquals(100, queueConfig.getNumRetry());
    assertFalse(queueConfig.updateRetryCount(100));
  }

  @Test
  public void updateVisibilityTimeout() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertTrue(queueConfig.updateVisibilityTimeout(1000L));
    assertEquals(1000L, queueConfig.getVisibilityTimeout());
    assertFalse(queueConfig.updateVisibilityTimeout(1000L));
  }

  @Test
  public void updateConcurrency() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    MinMax<Integer> concurrency = new MinMax<>(1, 10);
    assertTrue(queueConfig.updateConcurrency(concurrency));
    assertEquals(concurrency, queueConfig.getConcurrency());
    assertFalse(queueConfig.updateConcurrency(concurrency));
  }

  @Test
  public void updatePriority() {
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
  public void isDeadLetterQueue() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test", "test-dlq");
    assertTrue(queueConfig.isDeadLetterQueue("test-dlq"));
    assertFalse(queueConfig.isDeadLetterQueue("test"));
  }

  @Test
  public void hasDeadLetterQueue() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertFalse(queueConfig.hasDeadLetterQueue());
    queueConfig = TestUtils.createQueueConfig("test", "test-dlq");
    assertTrue(queueConfig.hasDeadLetterQueue());
  }

  @Test
  public void updatePriorityGroup() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    assertTrue(queueConfig.updatePriorityGroup("new-group"));
    assertEquals("new-group", queueConfig.getPriorityGroup());
    assertFalse(queueConfig.updatePriorityGroup("new-group"));
  }

  @Test
  public void equals() {
    QueueConfig queueConfig = TestUtils.createQueueConfig("test");
    QueueConfig queueConfig2 = TestUtils.createQueueConfig("test");
    assertEquals(queueConfig, queueConfig2);
  }
}
