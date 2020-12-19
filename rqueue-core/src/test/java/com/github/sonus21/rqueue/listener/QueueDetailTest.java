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

package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.models.db.DeadLetterQueue;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Tags;
import org.junit.jupiter.api.Test;

@CoreUnitTest
class QueueDetailTest extends TestBase {

  @Test
   void isDlqSet() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    assertFalse(queueDetail.isDlqSet());
    QueueDetail queueDetail2 = TestUtils.createQueueDetail("test", "test-dlq");
    assertTrue(queueDetail2.isDlqSet());
  }

  @Test
   void toConfig() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test");
    QueueConfig expectedConfig =
        QueueConfig.builder()
            .name(queueDetail.getName())
            .numRetry(queueDetail.getNumRetry())
            .queueName(queueDetail.getQueueName())
            .delayedQueueName(queueDetail.getDelayedQueueName())
            .processingQueueName(queueDetail.getProcessingQueueName())
            .visibilityTimeout(queueDetail.getVisibilityTimeout())
            .deadLetterQueues(new LinkedList<>())
            .concurrency(queueDetail.getConcurrency().toMinMax())
            .priority(queueDetail.getPriority())
            .priorityGroup(queueDetail.getPriorityGroup())
            .systemGenerated(queueDetail.isSystemGenerated())
            .build();

    QueueConfig queueConfig = queueDetail.toConfig();
    assertNotNull(queueConfig.getUpdatedOn());
    assertNotNull(queueConfig.getCreatedOn());
    assertNull(queueConfig.getDeletedOn());
    assertFalse(queueConfig.isDeleted());
    queueConfig.setUpdatedOn(null);
    queueConfig.setCreatedOn(null);
    assertEquals(expectedConfig, queueConfig);

    QueueDetail queueDetail2 = TestUtils.createQueueDetail("test", "test-dlq");
    QueueConfig queueConfig1 = queueDetail2.toConfig();
    assertEquals(
        Collections.singletonList(new DeadLetterQueue("test-dlq", false)),
        queueConfig1.getDeadLetterQueues());
  }

  @Test
   void expandQueueDetail() {
    Map<String, Integer> priority = new HashMap<>();
    priority.put("critical", 10);
    priority.put("high", 5);
    QueueDetail queueDetail = TestUtils.createQueueDetail("test", priority, 3, 1000L, null);
    List<QueueDetail> queueDetails = queueDetail.expandQueueDetail(true, -1);
    priority.put(Constants.DEFAULT_PRIORITY_KEY, 5);
    QueueDetail queueDetail2 =
        QueueDetail.builder()
            .name(queueDetail.getName() + "_critical")
            .numRetry(queueDetail.getNumRetry())
            .queueName(queueDetail.getQueueName() + "_critical")
            .delayedQueueName(queueDetail.getDelayedQueueName() + "_critical")
            .delayedQueueChannelName(queueDetail.getDelayedQueueChannelName() + "_critical")
            .processingQueueName(queueDetail.getProcessingQueueName() + "_critical")
            .processingQueueChannelName(queueDetail.getProcessingQueueChannelName() + "_critical")
            .visibilityTimeout(queueDetail.getVisibilityTimeout())
            .concurrency(queueDetail.getConcurrency())
            .priority(Collections.singletonMap(Constants.DEFAULT_PRIORITY_KEY, 10))
            .priorityGroup(queueDetail.getName())
            .active(true)
            .systemGenerated(true)
            .build();

    QueueDetail queueDetail3 =
        QueueDetail.builder()
            .name(queueDetail.getName() + "_high")
            .numRetry(queueDetail.getNumRetry())
            .queueName(queueDetail.getQueueName() + "_high")
            .delayedQueueName(queueDetail.getDelayedQueueName() + "_high")
            .delayedQueueChannelName(queueDetail.getDelayedQueueChannelName() + "_high")
            .processingQueueName(queueDetail.getProcessingQueueName() + "_high")
            .processingQueueChannelName(queueDetail.getProcessingQueueChannelName() + "_high")
            .visibilityTimeout(queueDetail.getVisibilityTimeout())
            .concurrency(queueDetail.getConcurrency())
            .priority(Collections.singletonMap(Constants.DEFAULT_PRIORITY_KEY, 5))
            .priorityGroup(queueDetail.getName())
            .systemGenerated(true)
            .active(true)
            .build();

    assertEquals(3, queueDetails.size());
    assertEquals(queueDetail3, queueDetails.get(0));
    assertEquals(queueDetail2, queueDetails.get(1));
    assertEquals(queueDetail, queueDetails.get(2));
  }
}
