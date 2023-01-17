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

package com.github.sonus21.rqueue.test;

import com.github.sonus21.rqueue.models.event.RqueueQueuePauseEvent;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@Getter
public class PauseUnpauseEventListener implements ApplicationListener<RqueueQueuePauseEvent> {

  private final List<RqueueQueuePauseEvent> eventList = new LinkedList<>();

  @Override
  public void onApplicationEvent(RqueueQueuePauseEvent event) {
    eventList.add(event);
  }
}
