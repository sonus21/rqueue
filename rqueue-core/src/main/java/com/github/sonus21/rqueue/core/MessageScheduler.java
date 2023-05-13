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

import static com.github.sonus21.rqueue.utils.Constants.MIN_DELAY;
import static java.lang.Math.min;

import com.github.sonus21.rqueue.config.RqueueConfig;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.slf4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
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

public abstract class MessageScheduler implements DisposableBean,
    ApplicationListener<RqueueBootstrapEvent> {

  private final Object monitor = new Object();
  @Autowired
  protected RqueueSchedulerConfig rqueueSchedulerConfig;
  @Autowired
  protected RqueueConfig rqueueConfig;
  private RedisScript<Long> redisScript;
  private MessageSchedulerListener messageSchedulerListener;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask;
  private Map<String, String> channelNameToQueueName;
  private Map<String, Long> queueNameToLastMessageScheduleTime;
  private ThreadPoolTaskScheduler scheduler;
  @Autowired
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;

  @Autowired
  @Qualifier("rqueueRedisLongTemplate")
  private RedisTemplate<String, Long> redisTemplate;

  private Map<String, QueueScheduler> queueSchedulers;
  private Map<String, Integer> errorCount;

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
      this.rqueueRedisListenerContainerFactory.addMessageListener(messageSchedulerListener,
          new ChannelTopic(channelName));
      channelNameToQueueName.put(channelName, queueName);
    }
  }

  private void startQueue(String queueName) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    if (scheduleTaskAtStartup() || !isRedisEnabled()) {
      schedule(queueName, getQueueStartTime(), false);
    }
    subscribeToRedisTopic(queueName);
  }

  protected long getQueueStartTime() {
    return System.currentTimeMillis() + MIN_DELAY;
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
    Assert.isTrue(queueRunningState.containsKey(queueName),
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

  private long getLastScheduleTime(String queueName) {
    return queueNameToLastMessageScheduleTime.getOrDefault(queueName, 0L);
  }

  protected ScheduledTaskDetail getScheduledTask(String queueName) {
    return queueNameToScheduledTask.get(queueName);
  }

  protected void schedule(String queueName, Long startTime, boolean forceSchedule) {
    this.queueSchedulers.get(queueName).schedule(queueName, startTime, forceSchedule);
  }

  protected void initialize() {
    List<String> queueNames = EndpointRegistry.getActiveQueues();
    defaultScriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
    redisScript = RedisScriptFactory.getScript(ScriptType.MOVE_EXPIRED_MESSAGE);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    channelNameToQueueName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToLastMessageScheduleTime = new ConcurrentHashMap<>(queueNames.size());
    queueSchedulers = new ConcurrentHashMap<>(queueNames.size());
    errorCount = new ConcurrentHashMap<>(queueNames.size());
    createScheduler(queueNames.size());
    if (isRedisEnabled()) {
      messageSchedulerListener = new MessageSchedulerListener();
    }
    for (String queueName : queueNames) {
      initQueue(queueName);
    }
  }

  private void initQueue(String queueName) {
    queueRunningState.put(queueName, false);
    queueSchedulers.put(queueName, new QueueScheduler());
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

  protected long getMinDelay() {
    return MIN_DELAY;
  }

  private class QueueScheduler {

    private void scheduleNewTask(QueueDetail queueDetail, String zsetName, long startTime) {
      MessageMoverTask task = new MessageMoverTask(queueDetail, zsetName);
      Future<?> future = scheduler.schedule(task, Instant.ofEpochMilli(startTime));
      addTask(task, new ScheduledTaskDetail(task.id, future, startTime));
    }

    private void scheduleTask(QueueDetail queueDetail, String zsetName, long startTime,
        long currentTime) {
      long requiredDelay = Math.max(1, startTime - currentTime);
      long taskStartTime = currentTime;
      MessageMoverTask task = new MessageMoverTask(queueDetail, zsetName);
      Future<?> future;
      if (requiredDelay < MIN_DELAY) {
        future = scheduler.submit(task);
      } else {
        taskStartTime = currentTime + requiredDelay;
        future = scheduler.schedule(task, Instant.ofEpochMilli(taskStartTime));
      }
      addTask(task, new ScheduledTaskDetail(task.id, future, taskStartTime));
    }

    private boolean shouldNotSchedule(String queueName, boolean forceSchedule) {
      boolean isQueueActive = isQueueActive(queueName);
      if (!isQueueActive || scheduler == null) {
        return true;
      }
      long lastSeenTime = getLastScheduleTime(queueName);
      long currentTime = System.currentTimeMillis();
      // ignore too frequents events
      return !forceSchedule && currentTime - lastSeenTime < getMinDelay();
    }

    private void handleTaskOverride(ScheduledTaskDetail scheduledTaskDetail,
        QueueDetail queueDetail, String zsetName, long startTime) {
      // we should not schedule too frequent calls
      long difference = startTime - scheduledTaskDetail.getStartTime();
      if (difference < getMinDelay()) {
        return;
      }
      long currentTime = System.currentTimeMillis();
      if (cancelExistingTask(scheduledTaskDetail, currentTime, queueDetail, zsetName)) {
        scheduleNewTask(queueDetail, zsetName, startTime);
      }
    }

    protected synchronized void schedule(String queueName, Long startTime, boolean forceSchedule) {
      getLogger().debug("Schedule Task queue={}, force={}", queueName, forceSchedule);
      if (shouldNotSchedule(queueName, forceSchedule)) {
        return;
      }
      long currentTime = System.currentTimeMillis();
      ScheduledTaskDetail scheduledTaskDetail = getScheduledTask(queueName);
      QueueDetail queueDetail = EndpointRegistry.get(queueName);
      String zsetName = getZsetName(queueName);
      // no task was scheduled or call came from existing MessageMoverTask
      if (scheduledTaskDetail == null || forceSchedule) {
        scheduleTask(queueDetail, zsetName, startTime, currentTime);
      } else {
        handleTaskOverride(scheduledTaskDetail, queueDetail, zsetName, startTime);
      }
    }

    private void addTask(MessageMoverTask task, ScheduledTaskDetail scheduledTaskDetail) {
      getLogger().debug("Adding Task task={}, startTime={}", task,
          scheduledTaskDetail.getStartTime());
      queueNameToLastMessageScheduleTime.put(task.getName(), System.currentTimeMillis());
      queueNameToScheduledTask.put(task.getName(), scheduledTaskDetail);
    }

    private boolean cancelExistingTask(ScheduledTaskDetail scheduledTaskDetail, long currentTime,
        QueueDetail queueDetail, String zsetName) {
      Future<?> submittedTask = scheduledTaskDetail.getFuture();
      boolean completedOrCancelled = submittedTask.isDone() || submittedTask.isCancelled();
      // this can happen when task was not scheduled due to some exception in scheduling
      if (completedOrCancelled) {
        return false;
      }
      // run existing tasks continue
      long existingDelay = scheduledTaskDetail.getStartTime() - currentTime;
      // task is about to run or was scheduled to run in last alive time, but could not so run it
      if (existingDelay < MIN_DELAY && existingDelay > Constants.TASK_ALIVE_TIME) {
        ThreadUtils.waitForTermination(getLogger(), submittedTask,
            Constants.DEFAULT_SCRIPT_EXECUTION_TIME, "LIST: {} ZSET: {}, Task: {} failed",
            queueDetail.getQueueName(), zsetName, scheduledTaskDetail);
        return false;
      }
      boolean cancelled = submittedTask.cancel(false);
      if (cancelled) {
        getLogger().debug("Task {} cancelled", scheduledTaskDetail.getId());
      }
      return cancelled;
    }
  }

  private class MessageMoverTask implements Runnable {

    private final String id;
    private final String name;
    private final String queueName;
    private final String zsetName;
    private final boolean processingQueue;

    MessageMoverTask(QueueDetail queueDetail, String zsetName) {
      this.id = UUID.randomUUID().toString();
      this.name = queueDetail.getName();
      this.queueName = queueDetail.getQueueName();
      this.zsetName = zsetName;
      this.processingQueue = isProcessingQueue(zsetName);
    }

    private long getNextScheduleTimeInternal(Long value, Exception e) {
      int errCount = 0;
      long nextTime;
      if (null != e) {
        errCount = errorCount.getOrDefault(name, 0) + 1;
        if (errCount % 3 == 0) {
          getLogger().error("Message mover task is failing continuously queue: {}", name, e);
        }
        long delay = (long) (MIN_DELAY * Math.pow(1.5, errCount));
        delay = Math.min(delay, rqueueSchedulerConfig.getMaxMessageMoverDelay());
        nextTime = System.currentTimeMillis() + delay;
      } else {
        nextTime = getNextScheduleTime(name, value);
      }
      errorCount.put(name, errCount);
      return nextTime;
    }

    @Override
    public String toString() {
      return String.format("MessageMoverTask(id=%s, queue=%s)", id, name);
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

    @Override
    public void run() {
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
        long nextExecutionTime = getNextScheduleTimeInternal(value, e);
        schedule(name, nextExecutionTime, true);
      }
    }

    public String getName() {
      return this.name;
    }
  }

  private class MessageSchedulerListener implements MessageListener {

    private void handleMessage(String queueName, Long startTime) {
      long lastSeenTime = getLastScheduleTime(queueName);
      long currentTime = System.currentTimeMillis();
      if (currentTime - lastSeenTime < getMinDelay()) {
        return;
      }
      long jobStartTime = getNextScheduleTime(queueName, startTime);
      schedule(queueName, jobStartTime, false);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
      if (message.getBody().length == 0 || message.getChannel().length == 0) {
        return;
      }
      String body = new String(message.getBody());
      String channel = new String(message.getChannel());
      getLogger().trace("Body: {} Channel: {}", body, channel);
      try {
        Long startTime = Long.parseLong(body);
        String queueName = channelNameToQueueName.get(channel);
        if (queueName == null) {
          getLogger().warn("Unknown channel name {}", channel);
          return;
        }
        handleMessage(queueName, startTime);
      } catch (Exception e) {
        getLogger().error("Error occurred on a channel {}, body: {}", channel, body, e);
      }
    }
  }

  @Getter
  private static class ScheduledTaskDetail {

    private final Future<?> future;
    private final long startTime;
    private final String id;

    ScheduledTaskDetail(String id, Future<?> future, long startTime) {
      this.startTime = startTime;
      this.future = future;
      this.id = id;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      if (future instanceof ScheduledFuture) {
        sb.append("ScheduledFuture(delay=");
        sb.append(((ScheduledFuture<?>) future).getDelay(TimeUnit.MILLISECONDS));
        sb.append("Ms, ");
      } else {
        sb.append("Future(");
      }
      sb.append("id=");
      sb.append(id);
      sb.append(", startTime=");
      sb.append(startTime);
      sb.append(", currentTime=");
      sb.append(System.currentTimeMillis());
      sb.append(")");
      return sb.toString();
    }
  }

}
