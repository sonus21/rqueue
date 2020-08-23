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

import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.List;

public interface RqueueMessageManager {
  /**
   * Very dangerous method it will delete all messages in a queue
   *
   * @param queueName queue name
   * @return fail/success
   */
  boolean deleteAllMessages(String queueName);

  default boolean deleteAllMessages(String queueName, String priority) {
    return deleteAllMessages(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  /**
   * Find all messages stored on a given queue, it considers all the messages including delayed and
   * non-delayed.
   *
   * @param queueName queue name to be query for
   * @return list of messages.
   */
  List<Object> getAllMessages(String queueName);

  /**
   * Find all messages stored on a given queue, it considers all the messages including delayed and
   * non-delayed.
   *
   * @param queueName queue name to be query for
   * @return list of messages.
   */
  default List<Object> getAllMessages(String queueName, String priority) {
    return getAllMessages(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  Object getMessage(String queueName, String id);

  default Object getMessage(String queueName, String priority, String id) {
    return getMessage(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }

  RqueueMessage getRqueueMessage(String queueName, String id);

  default RqueueMessage getRqueueMessage(String queueName, String priority, String id) {
    return getRqueueMessage(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }

  boolean exist(String queueName, String id);

  default boolean exist(String queueName, String priority, String id) {
    return exist(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }

  boolean deleteMessage(String queueName, String id);

  default boolean deleteMessage(String queueName, String priority, String id) {
    return deleteMessage(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }
}
