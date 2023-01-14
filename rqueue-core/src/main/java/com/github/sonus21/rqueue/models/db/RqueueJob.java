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

package com.github.sonus21.rqueue.models.db;

import static com.github.sonus21.rqueue.utils.ExceptionUtils.getTraceback;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.utils.Constants;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@ToString(callSuper = true)
public class RqueueJob extends SerializableBase {

  private static final long serialVersionUID = 6219118148061766036L;
  private String id;

  private String messageId;

  //  being consumed message
  private RqueueMessage rqueueMessage;
  // Message metadata for this message, metadata can have different RqueueMessage than currently
  // being consumed
  private MessageMetadata messageMetadata;
  // Status of this job
  private JobStatus status;
  // when this job was last checkin
  private long lastCheckinAt;
  // all checkins
  private List<CheckinMessage> checkins;
  // all executions for this job
  private List<Execution> executions;
  // stack trace
  private String error;
  // any error occurred during execution
  @JsonIgnore
  private Throwable exception;
  // when this job was created
  private long createdAt;
  // whe this job was updated last time
  private long updatedAt;

  public RqueueJob(
      String id, RqueueMessage rqueueMessage, MessageMetadata messageMetadata, Throwable error) {
    this(
        id,
        rqueueMessage.getId(),
        rqueueMessage,
        messageMetadata,
        JobStatus.CREATED,
        0,
        new LinkedList<>(),
        new LinkedList<>(),
        null,
        null,
        System.currentTimeMillis(),
        System.currentTimeMillis());
    if (error != null) {
      updateError(error);
      this.status = JobStatus.FAILED;
    }
  }

  private void updateError(Throwable e) {
    this.exception = e;
    this.error = getTraceback(e, Constants.MAX_STACKTRACE_LENGTH);
  }

  public void checkIn(Serializable message) {
    synchronized (this) {
      long checkInTime = System.currentTimeMillis();
      this.checkins.add(new CheckinMessage(message, checkInTime));
      this.lastCheckinAt = Math.max(checkInTime, lastCheckinAt);
      this.updatedAt = Math.max(checkInTime, updatedAt);
      this.notifyAll();
    }
  }

  public Execution startNewExecution() {
    Execution execution =
        new Execution(System.currentTimeMillis(), 0, null, null, ExecutionStatus.IN_PROGRESS);
    this.executions.add(execution);
    return execution;
  }

  public void updateExecutionStatus(ExecutionStatus status, Throwable e) {
    Execution execution = this.executions.get(this.executions.size() - 1);
    execution.setStatus(status);
    execution.setError(getTraceback(e, Constants.MAX_STACKTRACE_LENGTH));
    execution.setException(e);
    execution.setEndTime(System.currentTimeMillis());
    if (e != null && exception == null) {
      updateError(e);
    }
  }
}
