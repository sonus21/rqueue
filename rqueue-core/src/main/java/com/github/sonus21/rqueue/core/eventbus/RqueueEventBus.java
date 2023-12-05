/*
 *  Copyright 2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core.eventbus;

import com.google.common.eventbus.EventBus;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;

public class RqueueEventBus {

  private final EventBus eventBus;
  private final ApplicationEventPublisher applicationEventPublisher;

  public RqueueEventBus(EventBus eventBus, ApplicationEventPublisher applicationEventPublisher) {
    this.eventBus = eventBus;
    this.applicationEventPublisher = applicationEventPublisher;
  }

  public void publish(ApplicationEvent event) {
    applicationEventPublisher.publishEvent(event);
    eventBus.post(event);
  }

  public void register(Object listener) {
    eventBus.register(listener);
  }

  public void unregister(Object listener) {
    eventBus.unregister(listener);
  }

}
