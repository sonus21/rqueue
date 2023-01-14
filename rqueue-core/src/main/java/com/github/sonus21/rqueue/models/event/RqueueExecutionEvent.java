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

package com.github.sonus21.rqueue.models.event;

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.listener.QueueDetail;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

/**
 * This event is generated once a message is consumed. It's generated in all cases whether execution
 * was success or fail.
 */
@Getter
public class RqueueExecutionEvent extends ApplicationEvent {

  private static final long serialVersionUID = -7762050873209497221L;
  // Queue Detail object
  @NotNull
  private final transient QueueDetail queueDetail;
  // Job corresponding to this execution
  @NotNull
  private final transient Job job;

  public RqueueExecutionEvent(Job job) {
    super(job.getQueueDetail());
    this.queueDetail = job.getQueueDetail();
    this.job = job;
  }
}
