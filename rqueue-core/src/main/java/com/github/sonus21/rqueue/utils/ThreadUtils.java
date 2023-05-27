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

package com.github.sonus21.rqueue.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public final class ThreadUtils {

  private ThreadUtils() {
  }

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
      String beanName, String threadPrefix, int corePoolSize, int maxPoolSize, int queueCapacity) {
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix(threadPrefix);
    threadPoolTaskExecutor.setBeanName(beanName);
    if (corePoolSize > 0) {
      threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
      threadPoolTaskExecutor.setAllowCoreThreadTimeOut(true);
      threadPoolTaskExecutor.setMaxPoolSize(Math.max(corePoolSize, maxPoolSize));
      threadPoolTaskExecutor.setQueueCapacity(queueCapacity);
    }
    threadPoolTaskExecutor.afterPropertiesSet();
    return threadPoolTaskExecutor;
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
    if (future == null || future.isCancelled() || future.isDone()) {
      return;
    }
    if (future instanceof ScheduledFuture) {
      ScheduledFuture<?> f = (ScheduledFuture<?>) future;
      if (f.getDelay(TimeUnit.MILLISECONDS) > Constants.MIN_DELAY) {
        f.cancel(false);
        return;
      }
    }
    waitForShutdown(log, future, waitTimeInMillis, msg, msgParams);
  }

  public static boolean waitForWorkerTermination(
      final Collection<QueueThreadPool> queueThreadPools, long waitTime) {
    long endTime = System.currentTimeMillis() + waitTime;
    List<QueueThreadPool> remaining = new ArrayList<>(queueThreadPools);
    while (System.currentTimeMillis() < endTime && !remaining.isEmpty()) {
      List<QueueThreadPool> newRemaining = new ArrayList<>();
      for (QueueThreadPool queueThreadPool : remaining) {
        if (!queueThreadPool.allTasksCompleted()) {
          newRemaining.add(queueThreadPool);
        }
      }
      if (!newRemaining.isEmpty()) {
        TimeoutUtils.sleep(10);
      }
      remaining = newRemaining;
    }
    return remaining.isEmpty();
  }

  public static String getWorkerName(String name) {
    String camelCase = StringUtils.getBeanName(name);
    return camelCase + "Listener";
  }
}
