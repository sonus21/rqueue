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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;
import org.junit.Test;

public class QueueConfigTest {

  @Test
  public void addDeadLetterQueue() {
    QueueConfig queueConfig = new QueueConfig();
    Set<String> queues = new HashSet<>();
    queues.add("test-dlq");
    assertTrue(queueConfig.addDeadLetterQueue("test-dlq"));
    assertEquals(queues, queueConfig.getDeadLetterQueues());
    assertFalse(queueConfig.addDeadLetterQueue("test-dlq"));
    assertEquals(queues, queueConfig.getDeadLetterQueues());
    assertTrue(queueConfig.addDeadLetterQueue("test2-dlq"));
    queues.add("test2-dlq");
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
  public void testDelay() {
    QueueConfig queueConfig = new QueueConfig();
    assertFalse(queueConfig.isDelayed());
    assertTrue(queueConfig.updateIsDelay(true));
    assertTrue(queueConfig.isDelayed());
    assertFalse(queueConfig.updateIsDelay(true));
    assertTrue(queueConfig.isDelayed());
    assertTrue(queueConfig.updateIsDelay(false));
    assertFalse(queueConfig.isDelayed());
  }

  @Test
  public void testConstruction() {
    QueueConfig queueConfig = new QueueConfig("__rq::q", "q", -1, true, 100L);
    assertTrue(queueConfig.isDelayed());
    assertEquals("__rq::q", queueConfig.getId());
    assertEquals("q", queueConfig.getName());
    assertEquals(100L, queueConfig.getVisibilityTimeout());
    assertEquals(-1, queueConfig.getNumRetry());
    assertNotNull(queueConfig.getDeadLetterQueues());
    assertNotNull(queueConfig.getUpdatedOn());
    assertNotNull(queueConfig.getCreatedOn());
    assertEquals(queueConfig.getUpdatedOn(), queueConfig.getCreatedOn());
  }
}
