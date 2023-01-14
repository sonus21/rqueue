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

import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class RqueueQueuePauseEvent extends ApplicationEvent {

  private final String queue;

  private final boolean paused;

  /**
   * Create a new ApplicationEvent.
   *
   * @param source the object on which the event initially occurred (never {@code null})
   * @param queue  queue that's getting (un)paused
   * @param paused a boolean flag that indicates whether it's going to paused or otherwise
   */
  public RqueueQueuePauseEvent(Object source, String queue, boolean paused) {
    super(source);
    this.queue = queue;
    this.paused = paused;
  }
}
