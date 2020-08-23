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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.List;

public interface RqueueEndpointManager {

  /**
   * Use this method to register any queue, that's only used for sending message.
   *
   * @param name name of the queue
   * @param priorities list of priorities to be used while sending message on this queue.
   */
  void registerQueue(String name, String... priorities);

  default boolean isQueueRegistered(String queueName, String priority) {
    return isQueueRegistered(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  boolean isQueueRegistered(String queueName);

  List<QueueDetail> getQueueConfig(String queueName);
}
