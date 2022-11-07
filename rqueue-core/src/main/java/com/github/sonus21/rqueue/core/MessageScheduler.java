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

package com.github.sonus21.rqueue.core;

import static com.github.sonus21.rqueue.utils.Constants.MAX_MESSAGES;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import lombok.AllArgsConstructor;
import lombok.ToString;
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

public abstract class MessageScheduler
    implements DisposableBean, ApplicationListener<RqueueBootstrapEvent> {

  private final Object monitor = new Object();
  @Autowired protected RqueueSchedulerConfig rqueueSchedulerConfig;
  @Autowired protected RqueueConfig rqueueConfig;
  private RedisScript<Long> redisScript;
  private MessageSchedulerListener messageSchedulerListener;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask;
  private Map<String, String> channelNameToQueueName;
  private Map<String, Long> queueNameToLastMessageScheduleTime;
  private ThreadPoolTaskScheduler scheduler;
  @Autowired private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;

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
      this.rqueueRedisListenerContainerFactory.addMessageListener(
          messageSchedulerListener, new ChannelTopic(channelName));
      channelNameToQueueName.put(channelName, queueName);
    }
  }

  private void startQueue(String queueName) {
    if (Boolean.TRUE.equals(queueRunningState.get(queueName))) {
      return;
    }
    queueRunningState.put(queueName, true);
    if (scheduleTaskAtStartup() || !isRedisEnabled()) {
      long scheduleAt = getQueueStartTime();
      schedule(queueName, scheduleAt, false);
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

    private void updateLastScheduleTime(String queueName, long time) {
      queueNameToLastMessageScheduleTime.put(queueName, time);
    }

    private void scheduleNewTask(
        QueueDetail queueDetail, String queueName, String zsetName, long startTime) {
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

    private void scheduleTask(
        long startTime, long currentTime, QueueDetail queueDetail, String zsetName) {
      long requiredDelay = Math.max(1, startTime - currentTime);
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

    protected synchronized void schedule(String queueName, Long startTime, boolean forceSchedule) {
      if (shouldNotSchedule(queueName, forceSchedule)) {
        return;
      }
      long currentTime = System.currentTimeMillis();
      updateLastScheduleTime(queueName, currentTime);
      ScheduledTaskDetail scheduledTaskDetail = getScheduledTask(queueName);
      QueueDetail queueDetail = EndpointRegistry.get(queueName);
      String zsetName = getZsetName(queueName);
      if (scheduledTaskDetail == null || forceSchedule) {
        scheduleTask(startTime, currentTime, queueDetail, zsetName);
        return;
      }
      checkExistingTask(scheduledTaskDetail, currentTime, queueDetail, zsetName);
      scheduleNewTask(queueDetail, queueName, zsetName, startTime);
    }

    private void addTask(MessageMoverTask timerTask, ScheduledTaskDetail scheduledTaskDetail) {
      getLogger().debug("Timer: {}, Task: {}", timerTask, scheduledTaskDetail);
      queueNameToScheduledTask.put(timerTask.getName(), scheduledTaskDetail);
    }

    private void checkExistingTask(
        ScheduledTaskDetail scheduledTaskDetail,
        long currentTime,
        QueueDetail queueDetail,
        String zsetName) {
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
    }
  }

  @ToString
  @AllArgsConstructor
  private class MessageMoverTask implements Runnable {

    private final String name;
    private final String queueName;
    private final String zsetName;
    private final boolean processingQueue;

    private long getNextScheduleTimeInternal(Long value, Exception e) {
      int errCount = 0;
      long nextTime;
      if (null != e) {
        errCount = errorCount.getOrDefault(queueName, 0) + 1;
        if (errCount % 3 == 0) {
          getLogger().error("Message mover task is failing continuously queue: {}", name, e);
        }
        long delay = (long) (100 * Math.pow(1.5, errCount));
        delay = Math.min(delay, rqueueSchedulerConfig.getMaxMessageMoverDelay());
        nextTime = System.currentTimeMillis() + delay;
      }else{
        nextTime = getNextScheduleTime(queueName, value);
      }
      errorCount.put(queueName, errCount);
      return nextTime;
    }

    @Override
    public void run() {
      getLogger().debug("Running {}", this);
      Long value = null;
      Exception e = null;
      try {
        if (isQueueActive(name)) {
          long currentTime = System.currentTimeMillis();
          value =
              defaultScriptExecutor.execute(
                  redisScript,
                  Arrays.asList(queueName, zsetName),
                  currentTime,
                  MAX_MESSAGES,
                  processingQueue ? 1 : 0);
        }
      } catch (RedisSystemException ex) {
        e = ex;
      } catch (Exception ex) {
        e = ex;
        getLogger().warn("Task execution failed for the queue: {}", getName(), e);
      } finally {
        if (isQueueActive(name)) {
          long nextExecutionTime = getNextScheduleTimeInternal(value, e);
          schedule(name, nextExecutionTime, true);
        }
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
      schedule(queueName, startTime, false);
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
}
