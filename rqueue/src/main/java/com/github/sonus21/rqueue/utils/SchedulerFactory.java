/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.utils;

import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class SchedulerFactory {
  public static ThreadPoolTaskScheduler createThreadPoolTaskScheduler(
      int poolSize, String threadPrefix, int terminationTime) {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(poolSize);
    scheduler.setThreadNamePrefix(threadPrefix);
    scheduler.setAwaitTerminationSeconds(terminationTime);
    scheduler.setRemoveOnCancelPolicy(true);
    scheduler.afterPropertiesSet();
    return scheduler;
  }
}
