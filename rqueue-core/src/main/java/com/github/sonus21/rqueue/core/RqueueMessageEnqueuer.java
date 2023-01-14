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

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.springframework.messaging.converter.MessageConverter;

/**
 * RqueueMessageEnqueuer enqueue message to Redis queue using different mechanism. Use any of the
 * methods from this interface to enqueue message to any queue. Queue must exist, if a queue does
 * not exist then it will throw an error of the
 * {@link com.github.sonus21.rqueue.exception.QueueDoesNotExist}. In such case register your queue
 * using {@link RqueueEndpointManager#registerQueue(String, String...)} method.
 *
 * <p>There are four types of interfaces in this
 *
 * <p>enqueueXYZ : Messages enqueue using this method shall be consumed as soon as possible
 *
 * <p>enqueueInXYZ: Messages enqueue using enqueueInXYZ shall be consumed once the given time is
 * elapsed, like in 30 seconds.
 *
 * <p>enqueueAtXYZ: Messages send using enqueueAtXYZ shall be consumed as soon as the given time is
 * reached for example 3PM tomorrow.
 *
 * <p>enqueueUniqueXYZ: This method enqueue unique messages on a queue. New messages overwrite the
 * existing message, the overwriting only works till the point message is not consumed, once message
 * is consumed it's of no use. Uniqueness is only inside the queue.
 *
 * @author Sonu Kumar
 */
public interface RqueueMessageEnqueuer {

  /**
   * Enqueue a message on given queue without any delay, consume as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @return message id on successful enqueue otherwise null.
   */
  String enqueue(String queueName, Object message);

  /**
   * Enqueue a message on given queue without any delay, consume as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param messageId message id
   * @param message   message object it could be any arbitrary object.
   * @return message was enqueue successfully or failed.
   */
  boolean enqueue(String queueName, String messageId, Object message);

  /**
   * Enqueue unique message on a given queue without any delay, consume as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param messageId the message id for uniqueness
   * @param message   message object it could be any arbitrary object.
   * @return message id on successful enqueue otherwise null.
   */
  boolean enqueueUnique(String queueName, String messageId, Object message);

  /**
   * Enqueue a message on the given queue with the given retry count. This message would not be
   * consumed more than the specified time due to failure in underlying systems.
   *
   * @param queueName  on which queue message has to be send
   * @param message    message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or send
   *                   to dead letter queue configured using {@link RqueueListener#numRetries()}
   * @return message id on successful enqueue otherwise null.
   */
  String enqueueWithRetry(String queueName, Object message, int retryCount);

  /**
   * Enqueue a message on the given queue with the given retry count. This message would not be
   * consumed more than the specified time due to failure in underlying systems.
   *
   * @param queueName  on which queue message has to be send
   * @param messageId  message id for this message.
   * @param message    message object it could be any arbitrary object.
   * @param retryCount how many times a message would be retried, before it can be discarded or send
   *                   to dead letter queue configured using {@link RqueueListener#numRetries()}
   * @return message was enqueue successfully or failed.
   */
  boolean enqueueWithRetry(String queueName, String messageId, Object message, int retryCount);

  /**
   * Enqueue a message on given queue, that will be consumed as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the priority for this message, like high, low, medium etc
   * @param message   message object it could be any arbitrary object.
   * @return message id on successful enqueue otherwise null.
   */
  String enqueueWithPriority(String queueName, String priority, Object message);

  /**
   * Enqueue a message on given queue, that will be consumed as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the priority for this message, like high, low, medium etc
   * @param messageId the message id for this message
   * @param message   message object it could be any arbitrary object.
   * @return message was enqueued successfully or not.
   */
  boolean enqueueWithPriority(String queueName, String priority, String messageId, Object message);

  /**
   * Enqueue unique message on given queue, that will be consumed as soon as possible.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the priority for this message, like high, low, medium etc
   * @param messageId the message id for this message
   * @param message   message object it could be any arbitrary object.
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueUniqueWithPriority(
      String queueName, String priority, String messageId, Object message) {
    return enqueueUnique(
        PriorityUtils.getQueueNameForPriority(queueName, priority), messageId, message);
  }

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse, for example process in 10 seconds
   *
   * @param queueName        on which queue message has to be send
   * @param message          message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milliseconds
   * @return message id on successful enqueue otherwise null.
   */
  String enqueueIn(String queueName, Object message, long delayInMilliSecs);

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse.
   *
   * @param queueName        on which queue message has to be send
   * @param messageId        the message id, using which this message will be identified
   * @param message          message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milliseconds
   * @return message was enqueue successfully or failed.
   */
  boolean enqueueIn(String queueName, String messageId, Object message, long delayInMilliSecs);

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be executed.
   * @return message id on successful enqueue otherwise null.
   */
  default String enqueueIn(String queueName, Object message, Duration delay) {
    return enqueueIn(queueName, message, delay.toMillis());
  }

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param messageId the message id, using which this message will be identified
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be executed.
   * @return success or failure.
   */
  default boolean enqueueIn(String queueName, String messageId, Object message, Duration delay) {
    return enqueueIn(queueName, messageId, message, delay.toMillis());
  }

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the specified delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be executed.
   * @param unit      unit of the delay
   * @return message id on successful enqueue otherwise null.
   */
  default String enqueueIn(String queueName, Object message, long delay, TimeUnit unit) {
    return enqueueIn(queueName, message, unit.toMillis(delay));
  }

  /**
   * Schedule a message on the given queue with the provided delay. It will be available to consume
   * as soon as the specified delay elapse.
   *
   * @param queueName on which queue message has to be send
   * @param messageId message id using which this message can be identified
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be executed.
   * @param unit      unit of the delay
   * @return success or failure.
   */
  default boolean enqueueIn(
      String queueName, String messageId, Object message, long delay, TimeUnit unit) {
    return enqueueIn(queueName, messageId, message, unit.toMillis(delay));
  }

  /**
   * Enqueue a message on given queue with delay, consume as soon as the scheduled is expired.
   *
   * @param queueName          on which queue message has to be send
   * @param messageId          the message id for uniqueness
   * @param message            message object it could be any arbitrary object.
   * @param delayInMillisecond total execution delay
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  boolean enqueueUniqueIn(
      String queueName, String messageId, Object message, long delayInMillisecond);

  /**
   * Enqueue a task that would be scheduled to run in the specified milliseconds.
   *
   * @param queueName        on which queue message has to be sent
   * @param message          message object it could be any arbitrary object.
   * @param retryCount       how many times a message would be retried, before it can be discarded
   *                         or sent to dead letter queue configured using
   *                         {@link RqueueListener#numRetries()} ()}
   * @param delayInMilliSecs delay in milliseconds, this message would be only visible to the
   *                         listener when number of millisecond has elapsed.
   * @return message id on successful enqueue otherwise {@literal null}
   */
  String enqueueInWithRetry(
      String queueName, Object message, int retryCount, long delayInMilliSecs);

  /**
   * Enqueue a task that would be scheduled to run in the specified milliseconds.
   *
   * @param queueName        on which queue message has to be sent
   * @param messageId        the message identifier
   * @param message          message object it could be any arbitrary object.
   * @param retryCount       how many times a message would be retried, before it can be discarded
   *                         or sent to dead letter queue configured using
   *                         {@link RqueueListener#numRetries()} ()}
   * @param delayInMilliSecs delay in milliseconds, this message would be only visible to the
   *                         listener when number of millisecond has elapsed.
   * @return message was enqueue successfully or failed.
   */
  boolean enqueueInWithRetry(
      String queueName, String messageId, Object message, int retryCount, long delayInMilliSecs);

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName        on which queue message has to be send
   * @param priority         the name of the priority level
   * @param message          message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milliseconds
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  default String enqueueInWithPriority(
      String queueName, String priority, Object message, long delayInMilliSecs) {
    return enqueueIn(
        PriorityUtils.getQueueNameForPriority(queueName, priority), message, delayInMilliSecs);
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName        on which queue message has to be send
   * @param priority         the name of the priority level
   * @param messageId        the message id
   * @param message          message object it could be any arbitrary object.
   * @param delayInMilliSecs delay in milliseconds
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueInWithPriority(
      String queueName, String priority, String messageId, Object message, long delayInMilliSecs) {
    return enqueueIn(
        PriorityUtils.getQueueNameForPriority(queueName, priority),
        messageId,
        message,
        delayInMilliSecs);
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be consumed.
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  default String enqueueInWithPriority(
      String queueName, String priority, Object message, Duration delay) {
    return enqueueInWithPriority(queueName, priority, message, delay.toMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param messageId the message id
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be consumed.
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueInWithPriority(
      String queueName, String priority, String messageId, Object message, Duration delay) {
    return enqueueInWithPriority(queueName, priority, messageId, message, delay.toMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be consumed.
   * @param unit      unit of the delay
   * @return message id on a successful enqueue otherwise {@literal null}.
   */
  default String enqueueInWithPriority(
      String queueName, String priority, Object message, long delay, TimeUnit unit) {
    return enqueueInWithPriority(queueName, priority, message, unit.toMillis(delay));
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param messageId the message id
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be consumed.
   * @param unit      unit of the delay
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueInWithPriority(
      String queueName,
      String priority,
      String messageId,
      Object message,
      long delay,
      TimeUnit unit) {
    return enqueueInWithPriority(queueName, priority, messageId, message, unit.toMillis(delay));
  }

  /**
   * Schedule unique message on the given queue at the provided time. It will be executed as soon as
   * the given delay is elapse.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param messageId the message id
   * @param message   message object it could be any arbitrary object.
   * @param delay     time to wait before it can be consumed.
   * @param unit      unit of the delay
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueUniqueInWithPriority(
      String queueName,
      String priority,
      String messageId,
      Object message,
      long delay,
      TimeUnit unit) {
    return enqueueUniqueIn(
        PriorityUtils.getQueueNameForPriority(queueName, priority),
        messageId,
        message,
        unit.toMillis(delay));
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName               on which queue message has to be send
   * @param message                 message object it could be any arbitrary object.
   * @param startTimeInMilliSeconds time at which this message has to be consumed.
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  default String enqueueAt(String queueName, Object message, long startTimeInMilliSeconds) {
    return enqueueIn(queueName, message, startTimeInMilliSeconds - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName               on which queue message has to be send
   * @param messageId               message id
   * @param message                 message object it could be any arbitrary object.
   * @param startTimeInMilliSeconds time at which this message has to be consumed.
   * @return message was enqueued successfully or failed.
   */
  default boolean enqueueAt(
      String queueName, String messageId, Object message, long startTimeInMilliSeconds) {
    return enqueueIn(
        queueName, messageId, message, startTimeInMilliSeconds - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @param starTime  time at which this message has to be consumed.
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  default String enqueueAt(String queueName, Object message, Instant starTime) {
    return enqueueAt(queueName, message, starTime.toEpochMilli());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param messageId the message id
   * @param message   message object it could be any arbitrary object.
   * @param starTime  time at which this message has to be consumed.
   * @return message was enqueued successfully or failed.
   */
  default boolean enqueueAt(String queueName, String messageId, Object message, Instant starTime) {
    return enqueueAt(queueName, messageId, message, starTime.toEpochMilli());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @param starTime  time at which this message has to be consumed.
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  default String enqueueAt(String queueName, Object message, Date starTime) {
    return enqueueAt(queueName, message, starTime.toInstant());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be available to consume as
   * soon as the given time is reached.
   *
   * @param queueName on which queue message has to be send
   * @param messageId the message id
   * @param message   message object it could be any arbitrary object.
   * @param starTime  time at which this message has to be consumed.
   * @return message was enqueued successfully or failed.
   */
  default boolean enqueueAt(String queueName, String messageId, Object message, Date starTime) {
    return enqueueAt(queueName, messageId, message, starTime.toInstant());
  }

  /**
   * Schedule unique messages on the given queue at the provided time. It will be available to
   * consume as soon as the given time is reached.
   *
   * @param queueName          on which queue message has to be send
   * @param message            message object it could be any arbitrary object.
   * @param messageId          a unique identifier for this message
   * @param timeInMilliSeconds time at which this message has to be consumed.
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueUniqueAt(
      String queueName, String messageId, Object message, long timeInMilliSeconds) {
    return enqueueUniqueIn(
        queueName, messageId, message, timeInMilliSeconds - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName              on which queue message has to be send
   * @param priority               the name of the priority level
   * @param message                message object it could be any arbitrary object.
   * @param startTimeInMilliSecond time at which the message would be consumed.
   * @return message id on successful enqueue otherwise {@literal null}.
   */
  default String enqueueAtWithPriority(
      String queueName, String priority, Object message, long startTimeInMilliSecond) {
    return enqueueInWithPriority(
        queueName, priority, message, startTimeInMilliSecond - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName              on which queue message has to be send
   * @param priority               the name of the priority level
   * @param messageId              the message id
   * @param message                message object it could be any arbitrary object.
   * @param startTimeInMilliSecond time at which the message would be consumed.
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueAtWithPriority(
      String queueName,
      String priority,
      String messageId,
      Object message,
      long startTimeInMilliSecond) {
    return enqueueInWithPriority(
        queueName,
        priority,
        messageId,
        message,
        startTimeInMilliSecond - System.currentTimeMillis());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param message   message object it could be any arbitrary object.
   * @param startTime time at which message is supposed to consume
   * @return message id on successful enqueue otherwise {@literal null}
   */
  default String enqueueAtWithPriority(
      String queueName, String priority, Object message, Instant startTime) {
    return enqueueAtWithPriority(queueName, priority, message, startTime.toEpochMilli());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param message   message object it could be any arbitrary object.
   * @param messageId the message id
   * @param instant   time at which message is supposed to consume
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueAtWithPriority(
      String queueName, String priority, String messageId, Object message, Instant instant) {
    return enqueueAtWithPriority(queueName, priority, messageId, message, instant.toEpochMilli());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param message   message object it could be any arbitrary object.
   * @param time      time at which message would be consumed.
   * @return a message id on successful enqueue otherwise {@literal null}
   */
  default String enqueueAtWithPriority(
      String queueName, String priority, Object message, Date time) {
    return enqueueAtWithPriority(queueName, priority, message, time.toInstant());
  }

  /**
   * Schedule a message on the given queue at the provided time. It will be executed as soon as the
   * given time is reached, time must be in the future.
   *
   * @param queueName on which queue message has to be send
   * @param priority  the name of the priority level
   * @param messageId the message id
   * @param message   message object it could be any arbitrary object.
   * @param time      time at which message would be consumed.
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueAtWithPriority(
      String queueName, String priority, String messageId, Object message, Date time) {
    return enqueueAtWithPriority(queueName, priority, messageId, message, time.toInstant());
  }

  /**
   * Schedule unique messages on the given queue at the provided time. It will be available to
   * consume as soon as the given time is reached.
   *
   * @param queueName          on which queue message has to be send
   * @param priority           priority of the given message
   * @param message            message object it could be any arbitrary object.
   * @param messageId          a unique identifier message id for this message
   * @param timeInMilliSeconds time at which this message has to be consumed.
   * @return message was enqueue successfully or failed.
   */
  default boolean enqueueUniqueAtWithPriority(
      String queueName,
      String priority,
      String messageId,
      Object message,
      long timeInMilliSeconds) {
    return enqueueUniqueInWithPriority(
        queueName,
        priority,
        messageId,
        message,
        timeInMilliSeconds - System.currentTimeMillis(),
        TimeUnit.MILLISECONDS);
  }

  /**
   * Enqueue a message on given queue that will be running after a given period. It works like
   * periodic cron that's scheduled at certain interval, for example every 30 seconds.
   *
   * @param queueName            on which queue message has to be send
   * @param message              message object it could be any arbitrary object.
   * @param periodInMilliSeconds period of this job in milliseconds.
   * @return message id on successful enqueue otherwise null.
   */
  String enqueuePeriodic(String queueName, Object message, long periodInMilliSeconds);

  /**
   * Enqueue a message on given queue that will be running after a given period. It works like
   * periodic cron that's scheduled at certain interval, for example every 30 seconds.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @param period    period of this job
   * @param unit      period unit
   * @return message id on successful enqueue otherwise null.
   */
  default String enqueuePeriodic(String queueName, Object message, long period, TimeUnit unit) {
    return enqueuePeriodic(queueName, message, unit.toMillis(period));
  }

  /**
   * Enqueue a message on given queue that will be running after a given period. It works like
   * periodic cron that's scheduled at certain interval, for example every 30 seconds.
   *
   * @param queueName on which queue message has to be send
   * @param message   message object it could be any arbitrary object.
   * @param period    job period
   * @return message id on successful enqueue otherwise null.
   */
  default String enqueuePeriodic(String queueName, Object message, Duration period) {
    return enqueuePeriodic(queueName, message, period.toMillis());
  }

  /**
   * Enqueue a message on given queue that will be running after a given period. It works like
   * periodic cron that's scheduled at certain interval, for example every 30 seconds.
   *
   * @param queueName            on which queue message has to be send
   * @param messageId            message id corresponding to this message
   * @param message              message object it could be any arbitrary object.
   * @param periodInMilliSeconds period of this job in milliseconds.
   * @return success or failure
   */
  boolean enqueuePeriodic(
      String queueName, String messageId, Object message, long periodInMilliSeconds);

  /**
   * Enqueue a message on given queue that will be running after a given period. It works like
   * periodic cron that's scheduled at certain interval, for example every 30 seconds.
   *
   * @param queueName on which queue message has to be send
   * @param messageId message id corresponding to this message
   * @param message   message object it could be any arbitrary object.
   * @param period    period of this job .
   * @param unit      unit of this period
   * @return success or failure
   */
  default boolean enqueuePeriodic(
      String queueName, String messageId, Object message, long period, TimeUnit unit) {
    return enqueuePeriodic(queueName, messageId, message, unit.toMillis(period));
  }

  /**
   * Enqueue a message on given queue that will be running after a given period. It works like
   * periodic cron that's scheduled at certain interval, for example every 30 seconds.
   *
   * @param queueName on which queue message has to be send
   * @param messageId message id corresponding to this message
   * @param message   message object it could be any arbitrary object.
   * @param period    period of this job .
   * @return success or failure
   */
  default boolean enqueuePeriodic(
      String queueName, String messageId, Object message, Duration period) {
    return enqueuePeriodic(queueName, messageId, message, period.toMillis());
  }

  MessageConverter getMessageConverter();
}
