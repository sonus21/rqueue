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

package com.github.sonus21.rqueue.utils;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Slf4j
public final class QueueThreadPool {
  private final AsyncTaskExecutor taskExecutor;
  private final boolean defaultExecutor;
  private final Semaphore semaphore;
  private final int maxThreadsCount;

  public QueueThreadPool(
      AsyncTaskExecutor taskExecutor, boolean defaultExecutor, int maxThreadsCount) {
    this.taskExecutor = taskExecutor;
    this.defaultExecutor = defaultExecutor;
    this.maxThreadsCount = maxThreadsCount;
    this.semaphore = new Semaphore(maxThreadsCount );
  }

  public void release() {
    semaphore.release();
  }

  public void release(int n) {
    semaphore.release(n);
  }

  public boolean acquire(int n, long timeout) throws InterruptedException {
    if (taskExecutor instanceof ThreadPoolTaskExecutor) {
      ThreadPoolTaskExecutor executor = ((ThreadPoolTaskExecutor) taskExecutor);
      log.info("Current active threads {}", executor.getActiveCount());
    }
    return semaphore.tryAcquire(n, timeout, TimeUnit.MILLISECONDS);
  }

  public void execute(Runnable r) {
    this.taskExecutor.execute(r);
  }

  public int availableThreads() {
    return semaphore.availablePermits();
  }

  public boolean allTasksCompleted() {
    int permits = availableThreads();
    if (permits > maxThreadsCount) {
      log.error("More number of release is called");
    }
    return permits >= maxThreadsCount;
  }

  public String destroy() {
    if (!defaultExecutor) {
      return null;
    }
    if (taskExecutor instanceof ThreadPoolTaskExecutor) {
      ThreadPoolTaskExecutor executor = ((ThreadPoolTaskExecutor) taskExecutor);
      executor.destroy();
      return executor.getThreadNamePrefix();
    }
    return null;
  }
}
