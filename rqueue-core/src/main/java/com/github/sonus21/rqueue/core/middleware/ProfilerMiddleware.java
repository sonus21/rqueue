/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core.middleware;

import com.github.sonus21.rqueue.core.Job;
import java.time.Duration;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;

/**
 * A profile middleware that measures the execution time of a job, a more sophisticated profiler
 * middleware can report latency to <a href="https://newrelic.com/">New Relic</a>, <a
 * href="https://datadoghq.com">Datadog</a>, or use <a href="https://micrometer.io/">Micrometer</a>
 * to report latency or any other data.
 */
@Slf4j
public class ProfilerMiddleware implements Middleware {

  /**
   * Report execution of the said job
   *
   * @param job           the running job
   * @param executionTime execution time
   */
  protected void report(Job job, Duration executionTime) {
    log.info(
        "Queue: {}, Job: {}  took {}Ms",
        job.getRqueueMessage().getQueueName(),
        job.getId(),
        executionTime.toMillis());
  }

  @Override
  public void handle(Job job, Callable<Void> next) throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      next.call();
    } finally {
      report(job, Duration.ofMillis(System.currentTimeMillis() - startTime));
    }
  }
}
