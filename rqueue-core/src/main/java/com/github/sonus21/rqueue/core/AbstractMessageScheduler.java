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

import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.RedisScriptFactory.ScriptType;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

public abstract class AbstractMessageScheduler implements MessageScheduler {
  @Autowired protected RqueueSchedulerConfig rqueueSchedulerConfig;
  private RedisScript<Long> redisScript;
  private MessageSchedulerListener messageSchedulerListener;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask;
  private Map<String, String> channelNameToQueueName;
  private Map<String, Long> queueNameToLastMessageSeenTime;
  private ThreadPoolTaskScheduler scheduler;
  @Autowired private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;

  @Autowired
  @Qualifier("rqueueRedisLongTemplate")
  private RedisTemplate<String, Long> redisTemplate;

  protected abstract Logger getLogger();

  protected abstract long getNextScheduleTime(String queueName, Long value);

  protected abstract String getChannelName(String queueName);

  protected abstract String getZsetName(String queueName);

  protected abstract String getThreadNamePrefix();

  protected abstract int getThreadPoolSize();

  protected abstract boolean isProcessingQueue(String queueName);

  private void doStart() {
    for (String queueName : queueRunningState.keySet()) {
      startQueue(queueName);
    }
  }

  private void subscribeToRedisTopic(String queueName) {
    if (isRedisEnabled()) {
      String channelName = getChannelName(queueName);
      getLogger().debug("Queue {} subscribe to channel {}", queueName, channelName);
      this.rqueueRedisListenerContainerFactory
          .getContainer()
          .addMessageListener(messageSchedulerListener, new ChannelTopic(channelName));
      channelNameToQueueName.put(channelName, queueName);
    }
  }

  private void startQueue(String queueName) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    if (scheduleTaskAtStartup() || !isRedisEnabled()) {
      long scheduleAt = System.currentTimeMillis() + MIN_DELAY;
      schedule(queueName, scheduleAt, false);
    }
    subscribeToRedisTopic(queueName);
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
          future.cancel(true);
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
    return rqueueSchedulerConfig.isAutoStart();
  }

  private boolean isRedisEnabled() {
    return rqueueSchedulerConfig.isRedisEnabled();
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

  private void addTask(MessageMoverTask timerTask, ScheduledTaskDetail scheduledTaskDetail) {
    getLogger().debug("Timer: {} Task {}", timerTask, scheduledTaskDetail);
    queueNameToScheduledTask.put(timerTask.getName(), scheduledTaskDetail);
  }

  protected synchronized void schedule(String queueName, Long startTime, boolean forceSchedule) {
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
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    String zsetName = getZsetName(queueName);

    if (scheduledTaskDetail == null || forceSchedule) {
      long requiredDelay = max(1, startTime - currentTime);
      long taskStartTime = startTime;
      MessageMoverTask timerTask =
          new MessageMoverTask(
              queueDetail.getName(),
              queueDetail.getQueueName(),
              zsetName,
              isProcessingQueue(queueDetail.getName()));
      Future<?> future;
      if (requiredDelay < MIN_DELAY) {
        future = scheduler.submit(timerTask);
        taskStartTime = currentTime;
      } else {
        future = scheduler.schedule(timerTask, Instant.ofEpochMilli(currentTime + requiredDelay));
      }
      addTask(timerTask, new ScheduledTaskDetail(taskStartTime, future));
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
      ThreadUtils.waitForTermination(
          getLogger(),
          submittedTask,
          Constants.DEFAULT_SCRIPT_EXECUTION_TIME,
          "LIST: {} ZSET: {}, Task: {} failed",
          queueDetail.getQueueName(),
          zsetName,
          scheduledTaskDetail);
    }
    // Run was succeeded or cancelled submit new one
    MessageMoverTask timerTask =
        new MessageMoverTask(
            queueDetail.getName(),
            queueDetail.getQueueName(),
            zsetName,
            isProcessingQueue(zsetName));
    Future<?> future =
        scheduler.schedule(
            timerTask, Instant.ofEpochMilli(getNextScheduleTime(queueName, startTime)));
    addTask(timerTask, new ScheduledTaskDetail(startTime, future));
  }

  protected void initialize() {
    List<String> queueNames = EndpointRegistry.getActiveQueues();
    defaultScriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
    redisScript = RedisScriptFactory.getScript(ScriptType.MOVE_EXPIRED_MESSAGE);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    channelNameToQueueName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToLastMessageSeenTime = new ConcurrentHashMap<>(queueNames.size());
    createScheduler(queueNames.size());
    if (isRedisEnabled()) {
      messageSchedulerListener = new MessageSchedulerListener();
    }
    for (String queueName : queueNames) {
      queueRunningState.put(queueName, false);
    }
  }

  @Override
  @Async
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    doStop();
    if (event.isStartup()) {
      if (EndpointRegistry.getActiveQueueCount() == 0) {
        getLogger().warn("No queues are configured");
        return;
      }
      initialize();
      doStart();
    }
  }

  @ToString
  @AllArgsConstructor
  private class MessageMoverTask implements Runnable {
    private final String name;
    private final String queueName;
    private final String zsetName;
    private final boolean processingQueue;

    @Override
    public void run() {
      getLogger().debug("Running {}", this);
      try {
        if (isQueueActive(name)) {
          long currentTime = System.currentTimeMillis();
          Long value =
              defaultScriptExecutor.execute(
                  redisScript,
                  Arrays.asList(queueName, zsetName),
                  currentTime,
                  MAX_MESSAGES,
                  processingQueue ? 1 : 0);
          long nextExecutionTime = getNextScheduleTime(name, value);
          schedule(name, nextExecutionTime, true);
        }
      } catch (RedisSystemException e) {
        // no op
      } catch (Exception e) {
        getLogger().warn("Task execution failed for the queue: {}", name, e);
      }
    }

    public String getName() {
      return this.name;
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
      getLogger().debug("Body: {} Channel: {}", body, channel);
      try {
        Long startTime = Long.parseLong(body);
        String queueName = channelNameToQueueName.get(channel);
        if (queueName == null) {
          getLogger().warn("Unknown channel name {}", channel);
          return;
        }
        schedule(queueName, startTime, false);
      } catch (Exception e) {
        getLogger().error("Error occurred on a channel {}, body: {}", channel, body, e);
      }
    }
  }
}
