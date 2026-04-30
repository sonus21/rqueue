/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
package com.github.sonus21.rqueue.redis.metrics;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetricsProvider;

/**
 * Redis-backed {@link RqueueQueueMetricsProvider}. Reads sizes off the same
 * {@link RqueueRedisTemplate} the rest of the Redis backend uses: pending and DLQ live in
 * Redis lists (LLEN); scheduled and in-flight live in sorted sets (ZCARD).
 */
public class RedisRqueueQueueMetricsProvider implements RqueueQueueMetricsProvider {

  private final RqueueRedisTemplate<String> redisTemplate;

  public RedisRqueueQueueMetricsProvider(RqueueRedisTemplate<String> redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  @Override
  public long getPendingMessageCount(String queueName) {
    QueueDetail q = EndpointRegistry.get(queueName);
    Long size = redisTemplate.getListSize(q.getQueueName());
    return size == null ? 0L : size;
  }

  @Override
  public long getScheduledMessageCount(String queueName) {
    QueueDetail q = EndpointRegistry.get(queueName);
    Long size = redisTemplate.getZsetSize(q.getScheduledQueueName());
    return size == null ? 0L : size;
  }

  @Override
  public long getProcessingMessageCount(String queueName) {
    QueueDetail q = EndpointRegistry.get(queueName);
    Long size = redisTemplate.getZsetSize(q.getProcessingQueueName());
    return size == null ? 0L : size;
  }

  @Override
  public long getDeadLetterMessageCount(String queueName) {
    QueueDetail q = EndpointRegistry.get(queueName);
    if (!q.isDlqSet()) {
      return 0L;
    }
    Long size = redisTemplate.getListSize(q.getDeadLetterQueueName());
    return size == null ? 0L : size;
  }
}
