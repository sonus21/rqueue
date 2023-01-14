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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.List;

/**
 * Rqueue end point manager, manages the end point related to Rqueue.
 *
 * <p>if a queue does not exist then an exception of the {@link
 * com.github.sonus21.rqueue.exception.QueueDoesNotExist} will be thrown. In such cases you can
 * register a queue using {@link RqueueEndpointManager#registerQueue(String, String...)}
 */
public interface RqueueEndpointManager {

  /**
   * Use this method to register any queue, that's only used for sending message.
   *
   * @param name       name of the queue
   * @param priorities list of priorities to be used while sending message on this queue.
   */
  void registerQueue(String name, String... priorities);

  /**
   * Check if a queue is registered.
   *
   * @param queueName queue that needs to be checked
   * @return yes/no
   */
  boolean isQueueRegistered(String queueName);

  /**
   * Check if a queue is registered.
   *
   * @param queueName queue that needs to be checked
   * @param priority  priority of the queue
   * @return yes/no
   */
  default boolean isQueueRegistered(String queueName, String priority) {
    return isQueueRegistered(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  /**
   * Get queue config for a queue
   *
   * @param queueName queue name for which configuration has to be fetched
   * @return list of queue detail
   */
  List<QueueDetail> getQueueConfig(String queueName);

  /**
   * Pause or unpause queue
   *
   * @param queueName queue that needs to be paused or unpause
   * @param pause     boolean flags that indicates whether we need to pause or unpause
   * @return success/fail
   */
  boolean pauseUnpauseQueue(String queueName, boolean pause);

  /**
   * Pause or unpause queue with said priority
   *
   * @param queueName queue that needs to be paused or unpause
   * @param priority  priority of this queue
   * @param pause     boolean flags that indicates whether we need to pause or unpause
   * @return success/fail
   */
  boolean pauseUnpauseQueue(String queueName, String priority, boolean pause);

  /**
   * Check whether a queue is paused or not
   *
   * @param queueName queue name that needs to be checked
   * @return true/false
   */
  boolean isQueuePaused(String queueName);

  /**
   * Check whether a queue with given priority is paused or not
   *
   * @param queueName queue name that needs to be checked
   * @param priority  priority of that queue
   * @return true/false
   */
  boolean isQueuePaused(String queueName, String priority);
}
