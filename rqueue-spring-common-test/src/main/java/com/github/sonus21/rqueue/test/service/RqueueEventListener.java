/*
 *  Copyright 2022 Sonu Kumar
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
package com.github.sonus21.rqueue.test.service;

import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class RqueueEventListener {

  private Queue<RqueueExecutionEvent> executionEvents = new ConcurrentLinkedQueue<>();

  public void clearQueue() {
    executionEvents.clear();
  }

  public int getEventCount() {
    return executionEvents.size();
  }

  @EventListener
  public void listen(RqueueExecutionEvent event) {
    executionEvents.add(event);
  }

}
