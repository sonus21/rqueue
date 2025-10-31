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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.core.context.Context;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.Execution;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import java.io.Serializable;
import java.time.Duration;

/**
 * On each execution Rqueue creates a job to track it's status and execution progress.
 *
 * <p>A job belongs to a single message poll, each message listener call creates an execution
 * {@link com.github.sonus21.rqueue.models.db.Execution} and that has a detail for specific
 * execution. Overall job status can be found using this job interface. This object is available via
 * {@link org.springframework.messaging.handler.annotation.Header} in listener method.
 */
public interface Job {

  /**
   * Get job id corresponding to a message
   *
   * @return string job id
   */
  String getId();

  /**
   * Get message id corresponding to this job
   *
   * @return message id
   */
  String getMessageId();

  /**
   * RqueueMessage that's consumed by this job
   *
   * @return an object of RqueueMessage
   */
  RqueueMessage getRqueueMessage();

  /**
   * Get raw message that was enqueued, it's serialized string of the enqueued object
   *
   * @return string never null
   */
  String getRawMessage();

  /**
   * Checkin allows you to display a message for long running tasks, so that you can see the
   * progress.
   *
   * <p>The checking message could be anything given message's <b>JSON</b>
   * serializable/deserializable. The message could be the current status of the job or anything
   * else.
   *
   * <p><b>NOTE:</b> Checkin is not allowed for periodic job
   *
   * @param message a serializable message
   */
  void checkIn(Serializable message);

  /**
   * Get current visibility timeout of this job, if visibility timeout has already elapsed than
   * return zero value
   *
   * @return remaining duration that this job can take, otherwise other listener will consume this
   * message
   */
  Duration getVisibilityTimeout();

  /**
   * Update the visibility timeout of this job, the delta duration would be added in the current
   * visibility timeout. For example to add 10 seconds call this with Duration.ofSeconds(10), to
   * subtract 10 seconds call this with Duration.ofSeconds(-10). This method returns when message
   * exist in the processing queue, if message is already processed then it would return false.
   *
   * @param deltaDuration the delta time that needs to added to the existing visibility timeout
   * @return true/false whether this operation was success or not
   */
  boolean updateVisibilityTimeout(Duration deltaDuration);

  /**
   * A message that was enqueued
   *
   * @return an object could be null if deserialization fail.
   */
  Object getMessage();

  /**
   * There are times when message can not be deserialized from the string to Object, this can happen
   * when class information is missing. In such cases only raw message is available but application
   * can add a middleware to deserialize such messages and set in this so that other middleware(s)
   * can use the correct message object. Some use cases could be
   *
   * <ul>
   *   <li>Message was serialized to JSON without class information
   *   <li>Message was serialized to XML without class information
   *   <li>Message was serialized to Protobuf
   *   <li>Message was serialized to MessagePack
   * </ul>
   *
   * @param message message object
   */
  void setMessage(Object message);

  /**
   * MessageMetadata corresponding the enqueued message
   *
   * @return message metadata object
   */
  MessageMetadata getMessageMetadata();

  /**
   * The current status of this job, the message can have different status.
   *
   * @return job status
   * @see MessageMetadata
   */
  JobStatus getStatus();

  /**
   * Any error detail, if it fails during execution
   *
   * @return an error object
   */
  Throwable getException();

  /**
   * Total execution time of the fetched RqueueMessages
   *
   * @return total execution time, that's sum of all listener method calls.
   */
  long getExecutionTime();

  /**
   * Queue detail on which this job was executing
   *
   * @return queue detail object
   */
  QueueDetail getQueueDetail();

  /**
   * Return latest execution data
   *
   * @return an execution object
   */
  Execution getLatestExecution();

  /**
   * Return job context
   *
   * @return job context
   */
  Context getContext();

  /**
   * Set job context
   *
   * @param context context object
   */
  void setContext(Context context);

  /**
   * Release this job back to the queue, the released job would be available for re-execution after
   * the duration time.
   *
   * @param status   job status
   * @param why      why do want to release this job
   * @param duration any positive duration
   */
  void release(JobStatus status, Serializable why, Duration duration);

  /**
   * Release this job back to queue, this job available for execution after one second.
   *
   * @param status what should be the job status
   * @param why    why do you want to delete this job
   */
  void release(JobStatus status, Serializable why);

  /**
   * Delete this job
   *
   * @param status what should be the job status
   * @param why    why do you want to delete this job
   */
  void delete(JobStatus status, Serializable why);

  /**
   * Return whether this job was deleted, this returns true in two cases
   *
   * <p>1. when message is deleted using {@link #delete(JobStatus, Serializable)} method
   *
   * <p>2. Job's status is terminal status.
   *
   * @return true/false
   */
  boolean isDeleted();

  /**
   * Whether this job is released back to the queue or not in two cases
   *
   * <p>When message was released using {@link #release(JobStatus, Serializable)} or {@link
   * #release(JobStatus, Serializable, Duration)}
   *
   * <p>job is failed, so it will be retried later
   *
   * @return true/false
   */
  boolean isReleased();

  /**
   * Reports true when this job is moved to dead letter queue
   *
   * @return true/false
   */
  boolean hasMovedToDeadLetterQueue();

  /**
   * Whether this job is discarded or not, a message is discarded when it's all retries are over and
   * no dead letter queue is configured.
   *
   * @return true/false
   */
  boolean isDiscarded();

  /**
   * Number of times this job has failed
   *
   * @return failure count
   */
  int getFailureCount();
}
