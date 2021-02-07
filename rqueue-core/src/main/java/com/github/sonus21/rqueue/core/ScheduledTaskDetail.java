/*
 *  Copyright 2021 Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;

@Getter
class ScheduledTaskDetail {

  private final Future<?> future;
  private final long startTime;
  private final String id;

  ScheduledTaskDetail(long startTime, Future<?> future) {
    this.startTime = startTime;
    this.future = future;
    this.id = UUID.randomUUID().toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (future instanceof ScheduledFuture) {
      sb.append("ScheduledFuture(delay=");
      sb.append(((ScheduledFuture) future).getDelay(TimeUnit.MILLISECONDS));
      sb.append("Ms, ");
    } else {
      sb.append("Future(");
    }
    sb.append("id=");
    sb.append(id);
    sb.append(", startTime=");
    sb.append(startTime);
    sb.append(", currentTime=");
    sb.append(System.currentTimeMillis());
    sb.append(")");
    return sb.toString();
  }
}
