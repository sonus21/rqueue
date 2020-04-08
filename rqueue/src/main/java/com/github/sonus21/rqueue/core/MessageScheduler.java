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

package com.github.sonus21.rqueue.core;

import static com.github.sonus21.rqueue.utils.Constants.MAX_MESSAGES;
import static com.github.sonus21.rqueue.utils.Constants.MIN_DELAY;
import static java.lang.Math.max;
import static java.lang.Math.min;

import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.event.QueueInitializationEvent;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.SchedulerFactory;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
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
import org.springframework.util.CollectionUtils;

public abstract class MessageScheduler
    implements DisposableBean, ApplicationListener<QueueInitializationEvent> {
  private final int poolSize;
  private final boolean scheduleTaskAtStartup;
  private final boolean redisEnabled;
  private RedisScript<Long> redisScript;
  private MessageSchedulerListener messageSchedulerListener;
  private RedisTemplate<String, Long> redisTemplate;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  protected Map<String, Long> queueNameToDelay;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask;
  private Map<String, String> channelNameToQueueName;
  private Map<String, String> queueNameToZsetName;
  private Map<String, Long> queueNameToLastMessageSeenTime;
  private ThreadPoolTaskScheduler scheduler;
  @Autowired private RedisMessageListenerContainer redisMessageListenerContainer;

  public MessageScheduler(
      RedisTemplate<String, Long> redisTemplate,
      int poolSize,
      boolean scheduleTaskAtStartup,
      boolean redisEnabled) {
    this.poolSize = poolSize;
    this.scheduleTaskAtStartup = scheduleTaskAtStartup;
    this.redisEnabled = redisEnabled;
    this.redisTemplate = redisTemplate;
  }

  protected abstract void initializeState(Map<String, QueueDetail> queueDetailMap);

  protected abstract Logger getLogger();

  protected abstract long getNextScheduleTime(String queueName, Long value);

  protected abstract String getChannelName(String queueName);

  protected abstract String getZsetName(String queueName);

  protected abstract String getThreadNamePrefix();

  protected abstract boolean isQueueValid(QueueDetail queueDetail);

  private void doStart() {
    for (String queueName : queueRunningState.keySet()) {
      startQueue(queueName);
    }
  }

  private void startQueue(String queueName) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    if (scheduleTaskAtStartup() || !isRedisEnabled()) {
      long scheduleAt = System.currentTimeMillis() + MIN_DELAY;
      schedule(queueName, getZsetName(queueName), scheduleAt, false);
    }
    if (isRedisEnabled()) {
      redisMessageListenerContainer.addMessageListener(
          messageSchedulerListener, new ChannelTopic(getChannelName(queueName)));
      channelNameToQueueName.put(getChannelName(queueName), queueName);
      queueNameToZsetName.put(queueName, getZsetName(queueName));
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
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> runningState : queueRunningState.entrySet()) {
      String queueName = runningState.getKey();
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

  private boolean scheduleTaskAtStartup() {
    return scheduleTaskAtStartup;
  }

  private boolean isRedisEnabled() {
    return redisEnabled;
  }

  @Override
  public void destroy() throws Exception {
    doStop();
    if (scheduler != null) {
      scheduler.destroy();
    }
  }

  private void createScheduler(int queueCount) {
    if (queueCount == 0) {
      return;
    }
    scheduler =
        SchedulerFactory.createThreadPoolTaskScheduler(
            min(poolSize, queueCount), getThreadNamePrefix(), 60);
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
    if (!completedOrCancelled
        && existingDelay < MIN_DELAY
        && existingDelay > Constants.TASK_ALIVE_TIME) {
      try {
        submittedTask.get(Constants.DEFAULT_SCRIPT_EXECUTION_TIME, TimeUnit.MILLISECONDS);
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
            timerTask, Instant.ofEpochMilli(getNextScheduleTime(queueName, startTime)));
    queueNameToScheduledTask.put(
        timerTask.getQueueName(), new ScheduledTaskDetail(startTime, future));
  }

  @SuppressWarnings("unchecked")
  private void initialize(Map<String, QueueDetail> queueDetailMap) {
    Set<String> queueNames = new HashSet<>();
    for (Entry<String, QueueDetail> entry : queueDetailMap.entrySet()) {
      String queueName = entry.getKey();
      QueueDetail queueDetail = entry.getValue();
      if (isQueueValid(queueDetail)) {
        queueNames.add(queueName);
      }
    }
    defaultScriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
    redisScript = (RedisScript<Long>) RedisScriptFactory.getScript(ScriptType.PUSH_MESSAGE);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    channelNameToQueueName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToZsetName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToLastMessageSeenTime = new ConcurrentHashMap<>(queueNames.size());
    createScheduler(queueNames.size());
    if (isRedisEnabled()) {
      messageSchedulerListener = new MessageSchedulerListener();
    }
    for (String queueName : queueNames) {
      queueRunningState.put(queueName, false);
    }
    initializeState(queueDetailMap);
  }

  @Override
  public void onApplicationEvent(QueueInitializationEvent event) {
    doStop();
    if (event.isStart()) {
      if (CollectionUtils.isEmpty(event.getQueueDetailMap())) {
        return;
      }
      initialize(event.getQueueDetailMap());
      doStart();
    }
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
                  redisScript, Arrays.asList(queueName, zsetName), currentTime, MAX_MESSAGES);
          long nextExecutionTime = getNextScheduleTime(queueName, value);
          schedule(queueName, zsetName, nextExecutionTime, true);
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
