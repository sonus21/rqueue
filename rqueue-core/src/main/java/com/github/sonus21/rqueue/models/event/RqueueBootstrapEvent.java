/*
 * Copyright (c) 2019-2023 Sonu Kumar
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
import lombok.ToString;
import org.springframework.context.ApplicationEvent;

/**
 * This event is generated at RqueueContainer startup and shutdown.
 *
 * <p>This event can be used for different purpose, like registering some queue once Rqueue has
 * started, deleting all queue messages, cleaning up some resources at shutdown etc.
 *
 * <p>{@link RqueueBootstrapEvent#start} is false means it's shutdown event otherwise it's
 * bootstrap event.
 */
@Getter
@ToString(callSuper = true)
public class RqueueBootstrapEvent extends ApplicationEvent {

  private static final long serialVersionUID = 1955427920805054136L;
  private final boolean start;

  public RqueueBootstrapEvent(Object source, boolean start) {
    super(source);
    this.start = start;
  }

  // whether this is startup event
  public boolean isStartup() {
    return start;
  }

  // whether this is a shutdown event
  public boolean isShutdown() {
    return !start;
  }
}
