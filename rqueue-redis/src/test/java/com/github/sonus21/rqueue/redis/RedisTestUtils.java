/*
 * Copyright (c) 2026 Sonu Kumar
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

package com.github.sonus21.rqueue.redis;

import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.Concurrency;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import java.util.HashMap;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Module-local copy of the few helpers from {@code rqueue-core}'s test {@code TestUtils} that the
 * relocated DAO-impl tests still need. Kept narrow on purpose; do not grow this without first
 * checking whether the helper truly belongs in {@code rqueue-test-util} (shared) instead.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RedisTestUtils {

  public static String getQueueConfigKey(String name) {
    return "__rq::q-config::" + name;
  }

  public static QueueConfig createQueueConfig(String name) {
    return createQueueConfig(name, 3, 900000L, null);
  }

  public static QueueConfig createQueueConfig(
      String name, int numRetry, long visibilityTimeout, String dlq) {
    QueueDetail detail = QueueDetail.builder()
        .name(name)
        .queueName("__rq::queue::" + name)
        .processingQueueName("__rq::p-queue::" + name)
        .processingQueueChannelName("__rq::p-channel::" + name)
        .scheduledQueueName("__rq::d-queue::" + name)
        .scheduledQueueChannelName("__rq::d-channel::" + name)
        .completedQueueName("__rq::c-queue::" + name)
        .numRetry(numRetry)
        .visibilityTimeout(visibilityTimeout)
        .deadLetterQueueName(dlq)
        .priority(new HashMap<>())
        .priorityGroup("")
        .concurrency(new Concurrency(-1, -1))
        .active(true)
        .build();
    QueueConfig config = detail.toConfig();
    config.setId(getQueueConfigKey(name));
    return config;
  }
}
