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

package com.github.sonus21.rqueue.core;

import static java.lang.Math.min;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

public abstract class MessageScheduler implements DisposableBean,
    ApplicationListener<RqueueBootstrapEvent> {

  private final Object monitor = new Object();
  @Autowired
  protected RqueueSchedulerConfig rqueueSchedulerConfig;
  @Autowired
  protected RqueueConfig rqueueConfig;
  private RedisScript<Long> redisScript;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  private Map<String, ScheduledFuture<?>> queueNameToScheduledTask;
  private Map<String, Long> queueNameToNextRunTime;

  @VisibleForTesting
  protected RedisScheduleTriggerHandler redisScheduleTriggerHandler;

  private ThreadPoolTaskScheduler scheduler;
  @Autowired
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;

  @Autowired
  @Qualifier("rqueueRedisLongTemplate")
  private RedisTemplate<String, Long> redisTemplate;
  private Map<String, Integer> errorCount;

  protected abstract Logger getLogger();

  protected abstract long getNextScheduleTime(String queueName, long currentTime, Long value);

  protected abstract String getChannelName(String queueName);

  protected abstract String getZsetName(String queueName);

  protected abstract String getThreadNamePrefix();

  protected abstract int getThreadPoolSize();

  protected Duration getPeriod() {
    long delay = rqueueSchedulerConfig.getScheduledMessageTimeIntervalInMilli();
    if (delay <= 0) {
      delay = Constants.MIN_SCHEDULE_INTERVAL;
    }
    return Duration.ofMillis(delay);
  }

  protected abstract boolean isProcessingQueue();

  private void doStart() {
    for (String queueName : queueRunningState.keySet()) {
      startQueue(queueName);
    }
  }

  private MessageMoverTask task(String queueName, boolean periodic) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    String zsetName = getZsetName(queueName);
    return new MessageMoverTask(queueDetail, zsetName, periodic);
  }

  protected void schedule(String queueName) {
    Runnable task = task(queueName, true);
    ScheduledFuture<?> future = scheduler.scheduleAtFixedRate(task, getPeriod());
    queueNameToScheduledTask.put(queueName, future);
  }

  private void startQueue(String queueName) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    if (rqueueSchedulerConfig.isAutoStart()) {
      schedule(queueName);
    }
    if (isRedisEnabled()) {
      redisScheduleTriggerHandler.startQueue(queueName);
    }
  }

  private void doStop() {
    if (CollectionUtils.isEmpty(queueRunningState)) {
      return;
    }
    for (Map.Entry<String, Boolean> runningStateByQueue : queueRunningState.entrySet()) {
      if (Boolean.TRUE.equals(runningStateByQueue.getValue())) {
        stopQueue(runningStateByQueue.getKey());
      }
    }
    waitForRunningQueuesToStop();
    queueNameToScheduledTask.clear();
    if (isRedisEnabled()) {
      redisScheduleTriggerHandler.stop();
    }
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> runningState : queueRunningState.entrySet()) {
      String queueName = runningState.getKey();
      ScheduledFuture<?> scheduledFuture = queueNameToScheduledTask.get(queueName);
      ThreadUtils.waitForTermination(
          getLogger(),
          scheduledFuture,
          rqueueSchedulerConfig.getTerminationWaitTime(),
          "An exception occurred while stopping scheduler queue '{}'",
          queueName
      );
    }
  }

  private void stopQueue(String queueName) {
    Assert.isTrue(queueRunningState.containsKey(queueName),
        "Queue with name '" + queueName + "' does not exist");
    queueRunningState.put(queueName, false);
  }

  private boolean isRedisEnabled() {
    return rqueueSchedulerConfig.isRedisEnabled();
  }

  @Override
  public void destroy() throws Exception {
    synchronized (monitor) {
      doStop();
      if (scheduler != null) {
        scheduler.destroy();
      }
      monitor.notifyAll();
    }
  }

  private void createScheduler(int queueCount) {
    if (queueCount == 0) {
      return;
    }
    int threadPoolSize = min(getThreadPoolSize(), queueCount);
    String threadNamePrefix = getThreadNamePrefix();
    int terminationTime = 60;
    scheduler = ThreadUtils.createTaskScheduler(threadPoolSize, threadNamePrefix, terminationTime);
  }

  private boolean isQueueActive(String queueName) {
    Boolean val = queueRunningState.get(queueName);
    if (val == null) {
      return false;
    }
    return val;
  }

  protected void initialize() {
    List<String> queueNames = EndpointRegistry.getActiveQueues();
    defaultScriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
    redisScript = RedisScriptFactory.getScript(ScriptType.MOVE_EXPIRED_MESSAGE);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    queueNameToNextRunTime = new ConcurrentHashMap<>(queueNames.size());
    errorCount = new ConcurrentHashMap<>(queueNames.size());
    createScheduler(queueNames.size());
    for (String queueName : queueNames) {
      initQueue(queueName);
    }
    if (isRedisEnabled()) {
      redisScheduleTriggerHandler = new RedisScheduleTriggerHandler(
          getLogger(),
          rqueueRedisListenerContainerFactory,
          rqueueSchedulerConfig,
          queueNames,
          this::addTask,
          this::getChannelName);
      redisScheduleTriggerHandler.initialize();
    }
  }

  private void initQueue(String queueName) {
    queueRunningState.put(queueName, false);
  }

  @Override
  @Async
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    synchronized (monitor) {
      doStop();
      if (!rqueueSchedulerConfig.isEnabled()) {
        getLogger().debug("Scheduler is not enabled");
        return;
      }
      if (rqueueConfig.isProducer()) {
        getLogger().debug("Producer mode");
        return;
      }
      if (event.isStartup()) {
        if (EndpointRegistry.getActiveQueueCount() == 0) {
          getLogger().warn("No queues are configured");
          return;
        }
        initialize();
        doStart();
      }
      monitor.notifyAll();
    }
  }

  protected Future<?> addTask(String queueName) {
    return scheduler.submit(task(queueName, false));
  }

  private class MessageMoverTask implements Runnable {

    private final String id;
    private final String name;
    private final String queueName;
    private final String zsetName;
    private final boolean processingQueue;
    private final boolean periodic;

    MessageMoverTask(QueueDetail queueDetail, String zsetName, boolean periodic) {
      this.id = UUID.randomUUID().toString();
      this.name = queueDetail.getName();
      this.queueName = queueDetail.getQueueName();
      this.zsetName = zsetName;
      this.periodic = periodic;
      this.processingQueue = isProcessingQueue();
    }

    private long getNextScheduleTimeInternal(Long value, long currentTime, Exception e) {
      int errCount = 0;
      long nextTime;
      if (null != e) {
        errCount = errorCount.getOrDefault(name, 0) + 1;
        if (errCount % 3 == 0) {
          getLogger().error("Message mover task is failing continuously queue: {}", name, e);
        }
        // delay = x * 1.5^errorCount
        double delay = rqueueSchedulerConfig.minMessageMoveDelay() * Math.pow(1.5, errCount);
        long maxDelay = Math.min((long) delay, rqueueSchedulerConfig.getMaxMessageMoverDelay());
        nextTime = currentTime + maxDelay;
      } else {
        nextTime = getNextScheduleTime(name, currentTime, value);
      }
      errorCount.put(name, errCount);
      return nextTime;
    }

    @Override
    public String toString() {
      return String.format("MessageMoverTask(id=%s, queue=%s, periodic=%s)", id, name, periodic);
    }

    private long getMessageCount() {
      return rqueueSchedulerConfig.getMaxMessageCount();
    }

    private List<String> scriptKeys() {
      return Arrays.asList(queueName, zsetName);
    }

    private Object[] scriptArgs() {
      long currentTime = System.currentTimeMillis();
      return new Object[]{currentTime, getMessageCount(), processingQueue ? 1 : 0};
    }

    private boolean shouldSkip(long currentTime) {
      Long nextRunTime = queueNameToNextRunTime.get(queueName);
      return nextRunTime != null && nextRunTime > currentTime;
    }

    @Override
    public void run() {
      long currentTime = System.currentTimeMillis();
      if (shouldSkip(currentTime)) {
        getLogger().debug("Skipped {}", this);
        return;
      }
      getLogger().debug("Running {}", this);
      Long value = null;
      Exception e = null;
      try {
        if (isQueueActive(name)) {
          value = defaultScriptExecutor.execute(redisScript, scriptKeys(), scriptArgs());
        }
      } catch (RedisSystemException ex) {
        e = ex;
      } catch (Exception ex) {
        e = ex;
        getLogger().warn("Task execution failed for the queue: {}", getName(), e);
      } finally {
        long nextExecutionTime = getNextScheduleTimeInternal(value, currentTime, e);
        queueNameToNextRunTime.put(queueName, nextExecutionTime);
      }
    }

    public String getName() {
      return this.name;
    }
  }

}
