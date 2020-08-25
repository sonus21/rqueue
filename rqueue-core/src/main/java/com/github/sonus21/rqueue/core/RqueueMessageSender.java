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
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.messaging.converter.MessageConverter;

/**
 * RqueueMessage sender sends message to redis using different mechanism. Use any of the methods
 * from this interface to send message over any queue. Queue must exist, if a queue does not exist
 * then it will throw an error of the {@link com.github.sonus21.rqueue.exception.QueueDoesNotExist}.
 *
 * <p>There're three types of interfaces in this 1. enqueueXYZ 2. enqueueInXYZ 3. enqueueAtXYZ
 *
 * <p>Messages send using enqueueXYZ shall be consume as soon as possible
 *
 * <p>Messages send using enqueueInXYZ shall be consumed once the given time is elapsed, like in 30
 * seconds.
 *
 * <p>Messages send using enqueueAtXYZ shall be consumed as soon as the given time is reached for
 * example 3PM tomorrow.
 *
 * @see RqueueMessageEnqueuer
 * @see RqueueMessageManager
 * @see RqueueEndpointManager
 * @author Sonu Kumar
 */
@Deprecated
public interface RqueueMessageSender {
  /**
   * Enqueue a message on given queue without any delay, consume as soon as possible.
   *
   * @deprecated migrate to {@link #enqueue(String, Object)}
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  default boolean put(String queueName, Object message) {
    return enqueue(queueName, message);
  }

  /**
   * Enqueue a message on given queue without any delay, consume as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  boolean enqueue(String queueName, Object message);

  /**
   * Enqueue a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  default boolean put(String queueName, Object message, long delayInMilliSecs) {
    return enqueueIn(queueName, message, delayInMilliSecs);
  }

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds
   * @return message was submitted successfully or failed.
   */
  boolean enqueueIn(String queueName, Object message, long delayInMilliSecs);

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delay time to wait before it can be executed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueIn(String queueName, Object message, Duration delay) {
    return enqueueIn(queueName, message, delay.toMillis());
  }

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the specified delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param delay time to wait before it can be executed.
   * @param unit unit of the delay
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueIn(String queueName, Object message, long delay, TimeUnit unit) {
    return enqueueIn(queueName, message, unit.toMillis(delay));
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param startTimeInMilliSeconds time at which this message has to be consumed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueAt(String queueName, Object message, long startTimeInMilliSeconds) {
    return enqueueIn(queueName, message, startTimeInMilliSeconds - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param starTime time at which this message has to be consumed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueAt(String queueName, Object message, Instant starTime) {
    return enqueueAt(queueName, message, starTime.toEpochMilli());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param starTime time at which this message has to be consumed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueAt(String queueName, Object message, Date starTime) {
    return enqueueAt(queueName, message, starTime.toInstant());
  }

  /**
   * Enqueue a message on the given queue with the given retry count. This message would not be
   * consumed more than the specified time due to failure in underlying systems.
   *
   * @deprecated migrate to {@link #enqueueWithPriority(String, String, Object)}
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or send
   *     to dead letter queue configured using {@link RqueueListener#numRetries()}
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  default boolean put(String queueName, Object message, int retryCount) {
    return enqueueWithRetry(queueName, message, retryCount);
  }

  /**
   * Enqueue a message on the given queue with the given retry count. This message would not be
   * consumed more than the specified time due to failure in underlying systems.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or send
   *     to dead letter queue configured using {@link RqueueListener#numRetries()}
   * @return message was submitted successfully or failed.
   */
  boolean enqueueWithRetry(String queueName, Object message, int retryCount);

  /**
   * Enqueue a message on the given queue that would be scheduled to run in the specified milli
   * seconds.
   *
   * @deprecated migrate to {@link #enqueueInWithRetry(String, Object, int, long)}
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#numRetries()} ()}
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  @Deprecated
  default boolean put(String queueName, Object message, int retryCount, long delayInMilliSecs) {
    return enqueueInWithRetry(queueName, message, retryCount, delayInMilliSecs);
  }

  /**
   * Enqueue a task that would be scheduled to run in the specified milli seconds.
   *
   * @param queueName on which queue message has to be send
   * @param message message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or sent
   *     to dead letter queue configured using {@link RqueueListener#numRetries()} ()}
   * @param delayInMilliSecs delay in milli seconds, this message would be only visible to the
   *     listener when number of millisecond has elapsed.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs);

  /**
   * Enqueue a message on given queue, that will be consumed as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param priority the priority name for this message
   * @param message message object it could be any arbitrary object.
   * @return message was submitted successfully or failed.
   */
  boolean enqueueWithPriority(String queueName, String priority, Object message);

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milli seconds
   * @return message was submitted successfully or failed.
   */
  boolean enqueueInWithPriority(
      String queueName, String priority, Object message, long delayInMilliSecs);

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param delay time to wait before it can be consumed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueInWithPriority(
      String queueName, String priority, Object message, Duration delay) {
    return enqueueInWithPriority(queueName, priority, message, delay.toMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param delay time to wait before it can be consumed.
   * @param unit unit of the delay
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueInWithPriority(
      String queueName, String priority, Object message, long delay, TimeUnit unit) {
    return enqueueInWithPriority(queueName, priority, message, unit.toMillis(delay));
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param startTimeInMilliSecond time at which the message would be consumed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, long startTimeInMilliSecond) {
    return enqueueInWithPriority(
        queueName, priority, message, startTimeInMilliSecond - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param startTime time at which message is supposed to consume
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, Instant startTime) {
    return enqueueAtWithPriority(queueName, priority, message, startTime.toEpochMilli());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority the name of the priority level
   * @param message message object it could be any arbitrary object.
   * @param startTime time at which message would be consumed.
   * @return message was submitted successfully or failed.
   */
  default boolean enqueueAtWithPriority(
      String queueName, String priority, Object message, Date startTime) {
    return enqueueAtWithPriority(queueName, priority, message, startTime.toInstant());
  }

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

  /**
   * Get all registered message converters.
   *
   * @return list of message converters.
   */
  MessageConverter getMessageConverter();

  /**
   * Use this method to register any queue, that's only used for sending message.
   *
   * @param name name of the queue
   * @param priorities list of priorities to be used while sending message on this queue.
   */
  void registerQueue(String name, String... priorities);
}
