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

package com.github.sonus21.rqueue.test;

import com.github.sonus21.rqueue.core.eventbus.RqueueEventBus;
import com.github.sonus21.rqueue.models.event.RqueueQueuePauseEvent;
import com.google.common.eventbus.Subscribe;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import org.springframework.stereotype.Component;

@Component
@Getter
public class PauseUnpauseEventListener {

  private final RqueueEventBus eventBus;

  public PauseUnpauseEventListener(RqueueEventBus eventBus) {
    this.eventBus = eventBus;
    eventBus.register(this);
  }

  private final List<RqueueQueuePauseEvent> eventList = new LinkedList<>();

  @Subscribe
  public void onApplicationEvent(RqueueQueuePauseEvent event) {
    eventList.add(event);
  }
}
