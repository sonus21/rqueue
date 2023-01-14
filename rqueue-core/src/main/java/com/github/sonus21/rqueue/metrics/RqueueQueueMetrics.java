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

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.listener.QueueDetail;

/**
 * This class reports queue message counter.
 *
 * <p>Count can be sent to some monitoring tool like Prometheus, influx db etc
 */
public class RqueueQueueMetrics {

  private final RqueueRedisTemplate<String> redisTemplate;

  public RqueueQueueMetrics(RqueueRedisTemplate<String> redisTemplate) {
    this.redisTemplate = redisTemplate;
  }

  /**
   * Get number of messages waiting for consumption
   *
   * @param queue queue name
   * @return -1 if queue is not registered otherwise message count
   */
  public long getPendingMessageCount(String queue) {
    try {
      QueueDetail queueDetail = EndpointRegistry.get(queue);
      return redisTemplate.getListSize(queueDetail.getQueueName());
    } catch (QueueDoesNotExist e) {
      return -1;
    }
  }

  /**
   * Get number of messages waiting in scheduled queue, these messages would move to pending queue
   * as soon as the scheduled time is reach.
   *
   * @param queue queue name
   * @return -1 if queue is not registered otherwise message count
   */
  public long getScheduledMessageCount(String queue) {
    try {
      QueueDetail queueDetail = EndpointRegistry.get(queue);
      return redisTemplate.getZsetSize(queueDetail.getScheduledQueueName());
    } catch (QueueDoesNotExist e) {
      return -1;
    }
  }

  /**
   * Get number of messages those are currently being processed
   *
   * @param queue queue name
   * @return -1 if queue is not registered otherwise message count
   */
  public long getProcessingMessageCount(String queue) {
    try {
      QueueDetail queueDetail = EndpointRegistry.get(queue);
      return redisTemplate.getZsetSize(queueDetail.getProcessingQueueName());
    } catch (QueueDoesNotExist e) {
      return -1;
    }
  }

  /**
   * Get number of messages waiting for consumption
   *
   * @param queue    queue name
   * @param priority priority of this queue
   * @return -1 if queue is not registered otherwise message count
   */
  public long getPendingMessageCount(String queue, String priority) {
    try {
      QueueDetail queueDetail = EndpointRegistry.get(queue, priority);
      return redisTemplate.getListSize(queueDetail.getQueueName());
    } catch (QueueDoesNotExist e) {
      return -1;
    }
  }

  /**
   * Get number of messages waiting in scheduled queue, these messages would move to pending queue
   * as soon as the scheduled time is reach.
   *
   * @param queue    queue name
   * @param priority priority of this queue
   * @return -1 if queue is not registered otherwise message count
   */
  public long getScheduledMessageCount(String queue, String priority) {
    try {
      QueueDetail queueDetail = EndpointRegistry.get(queue, priority);
      return redisTemplate.getZsetSize(queueDetail.getScheduledQueueName());
    } catch (QueueDoesNotExist e) {
      return -1;
    }
  }

  /**
   * Get number of messages those are currently being processed
   *
   * @param queue    queue name
   * @param priority priority of this queue
   * @return -1 if queue is not registered otherwise message count
   */
  public long getProcessingMessageCount(String queue, String priority) {
    try {
      QueueDetail queueDetail = EndpointRegistry.get(queue, priority);
      return redisTemplate.getZsetSize(queueDetail.getProcessingQueueName());
    } catch (QueueDoesNotExist e) {
      return -1;
    }
  }
}
