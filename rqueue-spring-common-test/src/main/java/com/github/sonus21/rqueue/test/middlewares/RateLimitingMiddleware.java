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

package com.github.sonus21.rqueue.test.middlewares;

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.support.Middleware;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RateLimitingMiddleware implements Middleware {

  private final Object monitor;
  private final int count;
  private int currentCount = 0;

  public RateLimitingMiddleware(int count) {
    this.count = count;
    this.monitor = new Object();
  }

  @Override
  public Middleware handle(Job job, Middleware next) {
    log.info("{}", job);
    synchronized (monitor) {
      if (this.count == currentCount) {
        return null;
      }
      this.currentCount += 1;
    }
    return next;
  }
}
