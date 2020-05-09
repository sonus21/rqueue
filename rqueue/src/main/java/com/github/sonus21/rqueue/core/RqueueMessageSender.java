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

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.List;
import org.springframework.messaging.converter.MessageConverter;

/**
 * RqueueMessageSender creates message writer that writes message to Redis and have different
 * methods to push message to redis based on the use case. It can submit a delayed message along
 * with number of retries.
 *
 * @author Sonu Kumar
 */
public interface RqueueMessageSender {
  /**
   * Submit a message on given queue without any delay, listener would try to consume this message
   * immediately but due to heavy load message consumption can be delayed if message producer rate
   * is higher than the rate at consumer consume the messages.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   * @deprecated since 1.5.0 migrate to {@link #enqueue(String, Object)}
   */
  @Deprecated
  boolean put(String queueName, Object message);

  /**
   * This is an extension to the method {@link #put(String, Object)}. By default container would try
   * to deliver the same message for {@link Integer#MAX_VALUE} times, but that can be either
   * overridden using {@link RqueueListener#numRetries()}, even that value can be overridden using
   * this method.
   *
   * @deprecated please move to {@link #enqueue(String, Object, int)}
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  boolean put(String queueName, Object message, int retryCount);

  /**
   * This is the extension to the method {@link #put(String, Object)}, in this we can specify when
   * this message would be visible to the consumer.
   *
   * @deprecated please migrate to {@link #enqueueIn(String, Object, long)}
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  boolean put(String queueName, Object message, long delayInMilliSecs);

  /**
   * Extension to the {@link #put(String, Object, long)}, as like other retry method we can override
   * number of times a listener would be retrying
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   * @deprecated since 1.5.0 please migrate to {@link #enqueueIn(String, Object, int, long)}
   */
  @Deprecated
  boolean put(String queueName, Object message, int retryCount, long delayInMilliSecs);

  /**
   * Submit a message on given queue without any delay, listener would try to consume this message
   * immediately but due to heavy load message consumption can be delayed if message producer rate
   * is higher than the rate at consumer consume the messages.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  boolean enqueue(String queueName, Object message);

  /**
   * Submit a message on given queue without any delay at the specified priority level.
   *
   * @param queueName on which queue message has to be send
   * @param priorityLevel priority level for this message
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  boolean enqueue(String queueName, String priorityLevel, Object message);

  /**
   * This is an extension to the method {@link #put(String, Object)}. By default container would try
   * to deliver the same message for {@link Integer#MAX_VALUE} times, but that can be either
   * overridden using {@link RqueueListener#numRetries()}, even that value can be overridden using
   * this method.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @return message was submitted successfully or failed.
   */
  boolean enqueue(String queueName, Object message, int retryCount);

  /**
   * This is the extension to the method {@link #put(String, Object)}, in this we can specify when
   * this message would be visible to the consumer.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueIn(String queueName, Object message, long delayInMilliSecs);

  /**
   * Submit a task to the consumer in a given queue at the given priority level.
   *
   * @param queueName on which queue message has to be send
   * @param priorityLevel name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueIn(String queueName, String priorityLevel, Object message, long delayInMilliSecs);

  /**
   * Enqueue a task that would be scheduled to run after N milli seconds.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#delayedQueue()}
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueIn(String queueName, Object message, int retryCount, long delayInMilliSecs);

  /**
   * Move messages from Dead Letter queue to the destination queue. This push the messages at the
   * FRONT of destination queue, so that it can be reprocessed as soon as possible.
   *
   * @param deadLetterQueueName dead letter queue name
   * @param queueName queue name
   * @param maxMessages number of messages to be moved by default move {@link
   *     Constants#MAX_MESSAGES} messages
   * @return success or failure.
   */
  boolean moveMessageFromDeadLetterToQueue(
      String deadLetterQueueName, String queueName, Integer maxMessages);

  /**
   * A shortcut to the method {@link #moveMessageFromDeadLetterToQueue(String, String, Integer)}
   *
   * @param deadLetterQueueName dead letter queue name
   * @param queueName queue name
   * @return success or failure
   */
  boolean moveMessageFromDeadLetterToQueue(String deadLetterQueueName, String queueName);

  /**
   * Very dangerous method it will delete all messages in a queue
   *
   * @param queueName queue name
   * @return fail/success
   */
  boolean deleteAllMessages(String queueName);

  /**
   * Get one or more registered message converters.
   *
   * @return registered message converters.
   */
  Iterable<? extends MessageConverter> getMessageConverters();

  /**
   * Find all messages stored on a given queue, it considers all the messages including delayed and
   * non-delayed.
   *
   * @param queueName queue name to be query for
   * @return list of messages.
   */
  List<Object> getAllMessages(String queueName);
}
