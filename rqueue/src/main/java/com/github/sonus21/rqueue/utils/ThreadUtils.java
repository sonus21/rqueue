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

import com.github.sonus21.rqueue.models.ThreadCount;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class ThreadUtils {
  private ThreadUtils() {}

  public static ThreadPoolTaskScheduler createTaskScheduler(
      int poolSize, String threadPrefix, int terminationTime) {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setBeanName(threadPrefix.substring(0, threadPrefix.length() - 1));
    scheduler.setPoolSize(poolSize);
    scheduler.setThreadNamePrefix(threadPrefix);
    scheduler.setAwaitTerminationSeconds(terminationTime);
    scheduler.setRemoveOnCancelPolicy(true);
    scheduler.afterPropertiesSet();
    return scheduler;
  }

  public static ThreadPoolTaskExecutor createTaskExecutor(
      String beanName, String threadPrefix, int corePoolSize, int maxPoolSize) {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(threadPrefix);
    threadPoolTaskExecutor.setBeanName(beanName);
    if (corePoolSize > 0) {
      threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
      threadPoolTaskExecutor.setMaxPoolSize(Math.max(corePoolSize, maxPoolSize));
      threadPoolTaskExecutor.setQueueCapacity(0);
      threadPoolTaskExecutor.afterPropertiesSet();
    }
    return threadPoolTaskExecutor;
  }

  public static ThreadCount getThreadCount(
      boolean onlySpinning, int queueSize, int maxWorkersRequired) {
    int corePoolSize = onlySpinning ? queueSize : 2 * queueSize;
    int maxPoolSize =
        onlySpinning ? queueSize : Math.max(corePoolSize, queueSize + maxWorkersRequired);
    return new ThreadCount(corePoolSize, maxPoolSize);
  }

  private static void waitForShutdown(
      Logger log, Future<?> future, long waitTimeInMillis, String msg, Object... msgParams) {
    boolean completedOrCancelled = future.isCancelled() || future.isDone();
    if (completedOrCancelled) {
      return;
    }
    try {
      future.get(waitTimeInMillis, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | TimeoutException | CancellationException e) {
      log.debug(msg, msgParams, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  public static void waitForTermination(
      Logger log, Future<?> future, long waitTimeInMillis, String msg, Object... msgParams) {
    if (future == null) {
      return;
    }
    boolean completedOrCancelled = future.isCancelled() || future.isDone();
    if (!completedOrCancelled) {
      if (future instanceof ScheduledFuture) {
        ScheduledFuture f = (ScheduledFuture) future;
        if (f.getDelay(TimeUnit.MILLISECONDS) > Constants.MIN_DELAY) {
          return;
        }
      }
    }
    waitForShutdown(log, future, waitTimeInMillis, msg, msgParams);
  }
}
