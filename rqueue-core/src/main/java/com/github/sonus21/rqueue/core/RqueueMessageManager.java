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

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.List;
import org.springframework.messaging.converter.MessageConverter;

/**
 * Rqueue Message Manager manages messages related to a queue.
 *
 * <p>One or more messages can be deleted from a queue, not only this we can delete entire queue,
 * that will delete messages related to a queue.
 *
 * <p>We can also check whether the given message is enqueued or not.
 */
public interface RqueueMessageManager {

  /**
   * Very dangerous method it will delete all messages in a queue
   *
   * @param queueName queue name
   * @return fail/success
   */
  boolean deleteAllMessages(String queueName);

  /**
   * Delete all message for the given that has some priority like high,medium and low
   *
   * @param queueName queue name
   * @param priority  the priority for the queue
   * @return fail/success
   */
  default boolean deleteAllMessages(String queueName, String priority) {
    return deleteAllMessages(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  /**
   * Find all messages stored on a given queue, it considers three types of messages
   *
   * <p>1. In-Progress/In-Flight messages 2. Scheduled messages 3. Waiting for execution
   *
   * @param queueName queue name to be query for
   * @return list of messages
   */
  List<Object> getAllMessages(String queueName);

  /**
   * Find all messages stored on a given queue with given priority, this method is extension to the
   * method {@link #getAllMessages(String)}
   *
   * @param queueName queue name to be query for
   * @param priority  the priority of the queue
   * @return list of enqueued messages.
   */
  default List<Object> getAllMessages(String queueName, String priority) {
    return getAllMessages(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  /**
   * Find the enqueued message, messages are deleted automatically post consumption, post
   * consumption message has a fixed lifetime.
   *
   * @param queueName queue name on which message was enqueued
   * @param id        message id
   * @return the enqueued message, it could be null if message is not found or it's deleted.
   * @see RqueueConfig
   */
  Object getMessage(String queueName, String id);

  /**
   * Extension to the method {@link #getMessage(String, String)}, this provides the message for the
   * priority queue.
   *
   * @param queueName queue name on which message was enqueued
   * @param priority  the priority of the queue
   * @param id        message id
   * @return the enqueued message, it could be null if message is not found or it's deleted.
   */
  default Object getMessage(String queueName, String priority, String id) {
    return getMessage(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }

  /**
   * Extension to method {@link #getMessage(String, String)}, instead of providing message it
   * returns true/false.
   *
   * @param queueName queue name on which message was enqueued
   * @param id        message id
   * @return whether the message exist or not
   */
  boolean exist(String queueName, String id);

  /**
   * Extension to the method {@link #exist(String, String)}, that checks message for priority
   * queue.
   *
   * @param queueName queue name on which message was enqueued
   * @param priority  priority of the given queue
   * @param id        message id
   * @return whether the message exist or not
   */
  default boolean exist(String queueName, String priority, String id) {
    return exist(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }

  /**
   * Extension to the method {@link #getMessage(String, String)}, this returns internal message.
   *
   * @param queueName queue name on which message was enqueued
   * @param id        message id
   * @return the enqueued message
   */
  RqueueMessage getRqueueMessage(String queueName, String id);

  /**
   * Extension to the method {@link #getRqueueMessage(String, String)}
   *
   * @param queueName queue name on which message was enqueued
   * @param priority  the priority of the queue
   * @param id        message id
   * @return the enqueued message
   */
  default RqueueMessage getRqueueMessage(String queueName, String priority, String id) {
    return getRqueueMessage(PriorityUtils.getQueueNameForPriority(queueName, priority), id);
  }

  /**
   * Extension to the method {@link #getAllMessages(String)} this returns internal message.
   *
   * @param queueName queue name on which message was enqueued
   * @return the enqueued message
   */
  List<RqueueMessage> getAllRqueueMessage(String queueName);

  /**
   * Extension to the method {@link #getAllRqueueMessage(String)}
   *
   * @param queueName queue name on which message was enqueued
   * @param priority  the priority of the queue
   * @return the enqueued message
   */
  default List<RqueueMessage> getAllRqueueMessage(String queueName, String priority) {
    return getAllRqueueMessage(PriorityUtils.getQueueNameForPriority(queueName, priority));
  }

  /**
   * Delete a message that's enqueued to the given queue
   *
   * @param queueName queue on which message was enqueued
   * @param messageId message id
   * @return success/failure
   */
  boolean deleteMessage(String queueName, String messageId);

  /**
   * Delete a message that's enqueued to a queue with some priority
   *
   * @param queueName queue on which message was enqueued
   * @param priority  priority of the message like high/low/medium
   * @param messageId messageId corresponding to this message
   * @return success/failure
   */
  default boolean deleteMessage(String queueName, String priority, String messageId) {
    return deleteMessage(PriorityUtils.getQueueNameForPriority(queueName, priority), messageId);
  }

  /**
   * Get currently configured message converter
   *
   * @return message converter that's used for message (de)serialization
   */
  MessageConverter getMessageConverter();

  /**
   * Move messages from Dead Letter queue to the destination queue. This push the messages at the
   * FRONT of destination queue, so that it can be reprocessed as soon as possible.
   *
   * @param deadLetterQueueName dead letter queue name
   * @param queueName           queue name
   * @param maxMessages         number of messages to be moved by default move
   *                            {@link Constants#MAX_MESSAGES} messages
   * @return success or failure.
   */
  boolean moveMessageFromDeadLetterToQueue(
      String deadLetterQueueName, String queueName, Integer maxMessages);

  /**
   * A shortcut to the method {@link #moveMessageFromDeadLetterToQueue(String, String, Integer)}
   *
   * @param deadLetterQueueName dead letter queue name
   * @param queueName           queue name
   * @return success or failure
   */
  boolean moveMessageFromDeadLetterToQueue(String deadLetterQueueName, String queueName);

}
