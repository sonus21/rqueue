/*
 * Copyright (c) 2019-2019, Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import static java.lang.Math.max;
import static java.lang.Math.min;

import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;

public abstract class MessageScheduler implements InitializingBean, DisposableBean, SmartLifecycle {
  private static final long DEFAULT_SCRIPT_EXECUTION_TIME = 5000L;
  private static final long MIN_DELAY = 10L;
  private static final int MAX_MESSAGE = 100;
  private static final long TASK_ALIVE_TIME = -30 * 1000L;

  private final Object monitor = new Object();
  private final int poolSize;
  private volatile boolean running = false;
  private boolean scheduleTaskAtStartup;
  private RedisScript<Long> redisScript;
  private MessageSchedulerListener messageSchedulerListener;
  private RedisTemplate<String, Long> redisTemplate;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask;
  private Map<String, String> channelNameToQueueName;
  private Map<String, String> queueNameToZsetName;
  private Map<String, Long> queueNameToLastMessageSeenTime;
  private ThreadPoolTaskScheduler scheduler;

  @Autowired private RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Autowired private RedisMessageListenerContainer redisMessageListenerContainer;

  public MessageScheduler(
      RedisTemplate<String, Long> redisTemplate, int poolSize, boolean scheduleTaskAtStartup) {
    this.poolSize = poolSize;
    this.scheduleTaskAtStartup = scheduleTaskAtStartup;
    this.redisTemplate = redisTemplate;
  }

  protected abstract Logger getLogger();

  protected abstract long getNextScheduleTime(long currentTime, Long value);

  protected abstract String getChannelName(String queueName);

  protected abstract String getZsetName(String queueName);

  protected abstract String getThreadNamePrefix();

  protected abstract boolean isQueueValid(ConsumerQueueDetail queueDetail);

  @Override
  public boolean isRunning() {
    synchronized (monitor) {
      return running;
    }
  }

  @Override
  public void start() {
    synchronized (monitor) {
      running = true;
      monitor.notifyAll();
    }
    doStart();
  }

  protected void doStart() {
    for (String queueName : queueRunningState.keySet()) {
      startQueue(queueName);
    }
  }

  private void startQueue(String queueName) {
    if (queueRunningState.containsKey(queueName) && queueRunningState.get(queueName)) {
      return;
    }
    queueRunningState.put(queueName, true);
    if (isScheduleTaskAtStartup()) {
      long scheduleAt = System.currentTimeMillis() + MIN_DELAY;
      schedule(queueName, getZsetName(queueName), scheduleAt, false);
    }
    redisMessageListenerContainer.addMessageListener(
        messageSchedulerListener, new ChannelTopic(getChannelName(queueName)));
    channelNameToQueueName.put(getChannelName(queueName), queueName);
    queueNameToZsetName.put(queueName, getZsetName(queueName));
  }

  @Override
  public void stop() {
    synchronized (monitor) {
      running = false;
      monitor.notifyAll();
    }
    doStop();
  }

  private void doStop() {
    for (Map.Entry<String, Boolean> runningStateByQueue : queueRunningState.entrySet()) {
      if (runningStateByQueue.getValue()) {
        stopQueue(runningStateByQueue.getKey());
      }
    }
    waitForRunningQueuesToStop();
    queueNameToScheduledTask.clear();
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> queueRunningState : queueRunningState.entrySet()) {
      String queueName = queueRunningState.getKey();
      ScheduledTaskDetail scheduledTaskDetail = queueNameToScheduledTask.get(queueName);
      if (scheduledTaskDetail != null) {
        Future<?> future = scheduledTaskDetail.getFuture();
        boolean completedOrCancelled = future.isCancelled() || future.isDone();
        if (!completedOrCancelled) {
          scheduledTaskDetail.getFuture().cancel(true);
        }
      }
    }
  }

  private void stopQueue(String queueName) {
    Assert.isTrue(
        queueRunningState.containsKey(queueName),
        "Queue with name '" + queueName + "' does not exist");
    queueRunningState.put(queueName, false);
  }

  private boolean isScheduleTaskAtStartup() {
    return scheduleTaskAtStartup;
  }

  @Override
  public void destroy() throws Exception {
    stop();
    if (scheduler != null) {
      scheduler.destroy();
    }
  }

  private void createScheduler(int queueCount) {
    if (queueCount == 0) {
      return;
    }
    scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(min(poolSize, queueCount));
    scheduler.setThreadNamePrefix(getThreadNamePrefix());
    scheduler.setAwaitTerminationSeconds(60);
    scheduler.setRemoveOnCancelPolicy(true);
    scheduler.afterPropertiesSet();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    Set<String> queueNames = new HashSet<>();
    for (Entry<String, ConsumerQueueDetail> entry :
        rqueueMessageListenerContainer.getRegisteredQueues().entrySet()) {
      String queueName = entry.getKey();
      ConsumerQueueDetail queueDetail = entry.getValue();
      if (isQueueValid(queueDetail)) {
        queueNames.add(queueName);
      }
    }
    defaultScriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
    messageSchedulerListener = new MessageSchedulerListener();
    redisScript = (RedisScript<Long>) RedisScriptFactory.getScript(ScriptType.PUSH_MESSAGE);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    channelNameToQueueName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToZsetName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToLastMessageSeenTime = new ConcurrentHashMap<>(queueNames.size());
    createScheduler(queueNames.size());
    for (String queueName : queueNames) {
      queueRunningState.put(queueName, false);
    }
  }

  private boolean isQueueActive(String queueName) {
    Boolean val = queueRunningState.get(queueName);
    if (val == null) {
      return false;
    }
    return val;
  }

  protected synchronized void schedule(
      String queueName, String zsetName, Long startTime, boolean forceSchedule) {
    boolean isQueueActive = isQueueActive(queueName);
    if (!isQueueActive || scheduler == null) {
      return;
    }
    long lastSeenTime = queueNameToLastMessageSeenTime.getOrDefault(queueName, 0L);
    long currentTime = System.currentTimeMillis();
    // ignore too frequents events
    if (!forceSchedule && currentTime - lastSeenTime < MIN_DELAY) {
      return;
    }
    queueNameToLastMessageSeenTime.put(queueName, currentTime);

    ScheduledTaskDetail scheduledTaskDetail = queueNameToScheduledTask.get(queueName);
    if (scheduledTaskDetail == null || forceSchedule) {
      long requiredDelay = max(1, startTime - currentTime);
      long taskStartTime = startTime;
      MessageMoverTask timerTask = new MessageMoverTask(queueName, zsetName);
      Future<?> future;
      if (requiredDelay < MIN_DELAY) {
        future = scheduler.submit(timerTask);
        taskStartTime = currentTime;
      } else {
        future = scheduler.schedule(timerTask, Instant.ofEpochMilli(currentTime + requiredDelay));
      }
      scheduledTaskDetail = new ScheduledTaskDetail(taskStartTime, future);
      queueNameToScheduledTask.put(timerTask.getQueueName(), scheduledTaskDetail);
      return;
    }
    // run existing tasks continue
    long existingDelay = scheduledTaskDetail.getStartTime() - currentTime;
    Future<?> submittedTask = scheduledTaskDetail.getFuture();
    boolean completedOrCancelled = submittedTask.isDone() || submittedTask.isCancelled();
    // tasks older than TASK_ALIVE_TIME are considered dead
    if (!completedOrCancelled && existingDelay < MIN_DELAY && existingDelay > TASK_ALIVE_TIME) {
      try {
        submittedTask.get(DEFAULT_SCRIPT_EXECUTION_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException | TimeoutException | CancellationException e) {
        getLogger().debug("{} task failed", scheduledTaskDetail, e);
      }
    }
    // Run was succeeded or cancelled submit new one
    MessageMoverTask timerTask = new MessageMoverTask(queueName, zsetName);
    Future<?> future =
        scheduler.schedule(
            timerTask,
            Instant.ofEpochMilli(getNextScheduleTime(System.currentTimeMillis(), startTime)));
    queueNameToScheduledTask.put(
        timerTask.getQueueName(), new ScheduledTaskDetail(startTime, future));
  }

  private class MessageMoverTask implements Runnable {
    private final String queueName;
    private final String zsetName;

    MessageMoverTask(String queueName, String zsetName) {
      this.queueName = queueName;
      this.zsetName = zsetName;
    }

    String getQueueName() {
      return queueName;
    }

    @Override
    public void run() {
      try {
        if (isQueueActive(queueName)) {
          long currentTime = System.currentTimeMillis();
          Long value =
              defaultScriptExecutor.execute(
                  redisScript, Arrays.asList(queueName, zsetName), currentTime, MAX_MESSAGE);
          schedule(
              queueName, zsetName, getNextScheduleTime(System.currentTimeMillis(), value), true);
        }
      } catch (RedisSystemException e) {
        // no op
      } catch (Exception e) {
        getLogger().warn("Task execution failed for queue: {}", queueName, e);
      }
    }
  }

  private class MessageSchedulerListener implements MessageListener {
    @Override
    public void onMessage(Message message, byte[] pattern) {
      if (message.getBody().length == 0 || message.getChannel().length == 0) {
        return;
      }
      String body = new String(message.getBody());
      String channel = new String(message.getChannel());
      getLogger().debug("Body: {}  Channel: {}", body, channel);
      try {
        Long startTime = Long.parseLong(body);
        String queueName = channelNameToQueueName.get(channel);
        if (queueName == null) {
          getLogger().warn("Unknown channel name {}", channel);
          return;
        }
        String zsetName = queueNameToZsetName.get(queueName);
        if (zsetName == null) {
          getLogger().warn("Unknown zset name {}", queueName);
          return;
        }
        schedule(queueName, zsetName, startTime, false);
      } catch (NumberFormatException e) {
        getLogger().error("Invalid data {} on a channel {}", body, channel);
      }
    }
  }
}
