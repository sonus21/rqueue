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
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;

/**
 * A simple logging middleware that logs queue and job id for visibility
 */
@Slf4j
public class LoggingMiddleware implements Middleware {

  @Override
  public void handle(Job job, Callable<Void> next) throws Exception {
    log.info("Queue: {}, JobId: {}", job.getRqueueMessage().getQueueName(), job.getId());
    next.call();
  }
}
