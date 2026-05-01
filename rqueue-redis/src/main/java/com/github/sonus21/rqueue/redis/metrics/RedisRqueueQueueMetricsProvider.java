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
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetricsProvider;
import java.util.function.ToLongFunction;

/**
 * Redis-backed {@link RqueueQueueMetricsProvider}. Reads sizes off the same
 * {@link RqueueRedisTemplate} the rest of the Redis backend uses: pending and DLQ live in
 * Redis lists (LLEN); scheduled and in-flight live in sorted sets (ZCARD).
 *
 * <p>Unknown queues (i.e. names not present in {@link EndpointRegistry}) yield {@code 0} rather
 * than propagating {@link QueueDoesNotExist}, so callers can use the values directly as gauge
 * readings without guarding every lookup.
 */
public class RedisRqueueQueueMetricsProvider implements RqueueQueueMetricsProvider {

  private final RqueueRedisTemplate<String> redisTemplate;

  public RedisRqueueQueueMetricsProvider(RqueueRedisTemplate<String> redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  private long readSize(String queueName, ToLongFunction<QueueDetail> reader) {
    try {
      return reader.applyAsLong(EndpointRegistry.get(queueName));
    } catch (QueueDoesNotExist e) {
      return 0L;
    }
  }

  private long readSize(String queueName, String priority, ToLongFunction<QueueDetail> reader) {
    try {
      return reader.applyAsLong(EndpointRegistry.get(queueName, priority));
    } catch (QueueDoesNotExist e) {
      return 0L;
    }
  }

  private long listSize(String key) {
    Long size = redisTemplate.getListSize(key);
    return size == null ? 0L : size;
  }

  private long zsetSize(String key) {
    Long size = redisTemplate.getZsetSize(key);
    return size == null ? 0L : size;
  }

  @Override
  public long getPendingMessageCount(String queueName) {
    return readSize(queueName, q -> listSize(q.getQueueName()));
  }

  @Override
  public long getScheduledMessageCount(String queueName) {
    return readSize(queueName, q -> zsetSize(q.getScheduledQueueName()));
  }

  @Override
  public long getProcessingMessageCount(String queueName) {
    return readSize(queueName, q -> zsetSize(q.getProcessingQueueName()));
  }

  @Override
  public long getDeadLetterMessageCount(String queueName) {
    return readSize(queueName, q -> q.isDlqSet() ? listSize(q.getDeadLetterQueueName()) : 0L);
  }

  @Override
  public long getPendingMessageCount(String queueName, String priority) {
    return readSize(queueName, priority, q -> listSize(q.getQueueName()));
  }

  @Override
  public long getScheduledMessageCount(String queueName, String priority) {
    return readSize(queueName, priority, q -> zsetSize(q.getScheduledQueueName()));
  }

  @Override
  public long getProcessingMessageCount(String queueName, String priority) {
    return readSize(queueName, priority, q -> zsetSize(q.getProcessingQueueName()));
  }
}
