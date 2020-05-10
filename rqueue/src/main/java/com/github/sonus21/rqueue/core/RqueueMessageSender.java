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
 * RqueueMessage sender sends message to redis using different mechanism. Use any of the methods
 * from this interface to send message over any queue. Queue must exist, if a queue does not exist
 * then it will throw an error of the {@link com.github.sonus21.rqueue.exception.QueueDoesNotExist}.
 *
 * <p>There're two types of interfaces in this 1. enqueueXYZ 2. enqueueInXYZ
 *
 * <p>enqueueXYZ method is used to send the message without any delay, while if any message send
 * using enqueueInXYZ would be delayed for the specified period. The last parameter of enqueueIn is
 * the delay from the current time. For example if you want to send a message and that should be
 * consumed in 10 seconds then use one of the enqueueIn with delay = 10000, delay in these methods
 * are in milli seconds.
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
   */
  @Deprecated
  boolean put(String queueName, Object message);

  boolean enqueue(String queueName, Object message);

  /**
   * This is the extension to the method {@link #enqueue(String, Object)}, in this we can specify
   * when this message would be visible to the consumer.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  boolean put(String queueName, Object message, long delayInMilliSecs);

  boolean enqueueIn(String queueName, Object message, long delayInMilliSecs);

  /**
   * This is an extension to the method {@link #enqueue(String, Object)}. By default container would
   * try to deliver the same message for {@link Integer#MAX_VALUE} times, but that can be either
   * overridden using {@link RqueueListener#numRetries()}, even that value can be overridden using
   * this method.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#numRetries()}
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  boolean put(String queueName, Object message, int retryCount);

  boolean enqueueWithRetry(String queueName, Object message, int retryCount);

  /**
   * Enqueue a task that would be scheduled to run after N milli seconds.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#numRetries()} ()}
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  boolean put(String queueName, Object message, int retryCount, long delayInMilliSecs);

  boolean enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs);

  /**
   * Submit a message on given queue without any delay, listener would try to consume this message
   * immediately but due to heavy load message consumption can be delayed if message producer rate
   * is higher than the rate at consumer consume the messages.
   *
   * @param queueName on which queue message has to be send
   * @param priority the priority name for this message
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueWithPriority(String queueName, String priority, Object message);

  /**
   * Submit a task to the given queue at the given priority level.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueInWithPriority(
      String queueName, String priority, Object message, long delayInMilliSecs);

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

  MessageConverter getMessageConverter();
}
