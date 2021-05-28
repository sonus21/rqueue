/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
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
 * <p>A job belongs to a single message poll, each message listener call creates an execution {@link
 * com.github.sonus21.rqueue.models.db.Execution} and that has a detail for specific execution.
 * Overall job status can be found using this job interface. This object is available via {@link
 * org.springframework.messaging.handler.annotation.Header} in listener method.
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
   *     message
   */
  Duration getVisibilityTimeout();

  /**
   * Update the visibility timeout of this job, the delta duration would be added in the current
   * visibility timeout. For example to add 10 seconds call this with {@value
   * Duration.ofSeconds(10)}, to subtract 10 seconds call this with {@value *
   * Duration.ofSeconds(-10)}.
   *
   * @param deltaDuration the delta time that needs to added to the existing visibility timeout
   */
  boolean updateVisibilityTimeout(Duration deltaDuration);

  /**
   * A message that was enqueued
   *
   * @return an object could be null if deserialization fail.
   */
  Object getMessage();

  /**
   * MessageMetadata corresponding the enqueued message
   *
   * @return message metadata object
   */
  MessageMetadata getMessageMetadata();

  /**
   * The current status of this job, the message can have different status.
   *
   * @see MessageMetadata
   * @return job status
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

  Execution getLatestExecution();

  Context getContext();

  void setContext(Context context);

  void release(JobStatus status, Serializable why, Duration duration);

  void release(JobStatus status, Serializable why);

  void delete(JobStatus status, Serializable why);

  boolean isDeleted();

  boolean isReleased();

  boolean hasMovedToDeadLetterQueue();

  boolean isDiscarded();

  int getFailureCount();
}
