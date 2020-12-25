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

package com.github.sonus21.rqueue.models.db;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class RqueueJob extends SerializableBase {
  private static final long serialVersionUID = 6219118148061766036L;
  private String id;

  private String messageId;

  // currently being consumed message
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
  // any error occurred during execution
  private Throwable error;
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
        error,
        System.currentTimeMillis(),
        System.currentTimeMillis());
    if (error != null) {
      this.status = JobStatus.FAILED;
    }
  }

  public void checkIn(Object message) {
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
        new Execution(System.currentTimeMillis(), 0, null, ExecutionStatus.IN_PROGRESS);
    this.executions.add(execution);
    return execution;
  }

  public void updateExecutionStatus(ExecutionStatus status, Throwable e) {
    Execution execution = this.executions.get(this.executions.size() - 1);
    execution.setStatus(status);
    execution.setError(e);
    execution.setEndTime(System.currentTimeMillis());
    if (e != null && error == null) {
      error = e;
    }
  }
}
