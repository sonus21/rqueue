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

package com.github.sonus21.rqueue.models.event;

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.TaskStatus;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.context.ApplicationEvent;

/**
 * This event is generated once a message is consumed. It's generated in all cases whether execution
 * was success or fail.
 */
@Getter
public class RqueueExecutionEvent extends ApplicationEvent {
  private static final long serialVersionUID = -7762050873209497221L;
  /** Task status for this job, it could be null when execution was failed. */
  @Nullable @Deprecated private final TaskStatus status;
  // Rqueue message that was consumed
  @NotNull @Deprecated private final RqueueMessage rqueueMessage;
  // MessageMetadata corresponding to this
  @NotNull @Deprecated private final MessageMetadata messageMetadata;
  // Queue Detail object
  @NotNull private final transient QueueDetail queueDetail;
  // Job corresponding to this execution
  @NotNull private final transient Job job;

  public RqueueExecutionEvent(Job job) {
    super(job.getQueueDetail());
    this.queueDetail = job.getQueueDetail();
    this.status = job.getMessageMetadata().getStatus().getTaskStatus();
    this.rqueueMessage = job.getRqueueMessage();
    this.messageMetadata = job.getMessageMetadata();
    this.job = job;
  }
}
