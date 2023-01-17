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

package com.github.sonus21.rqueue.models.aggregator;

import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.ArrayList;
import java.util.List;

public class QueueEvents {

  public final List<RqueueExecutionEvent> rqueueExecutionEvents;
  private final Long createdAt;

  public QueueEvents(RqueueExecutionEvent event) {
    createdAt = System.currentTimeMillis();
    rqueueExecutionEvents = new ArrayList<>();
    addEvent(event);
  }

  public void addEvent(RqueueExecutionEvent event) {
    rqueueExecutionEvents.add(event);
  }

  public boolean processingRequired(int maxWaitTimeInSeconds, int maxEvents) {
    return (System.currentTimeMillis() - createdAt) >= maxWaitTimeInSeconds * Constants.ONE_MILLI
        || rqueueExecutionEvents.size() >= maxEvents;
  }
}
