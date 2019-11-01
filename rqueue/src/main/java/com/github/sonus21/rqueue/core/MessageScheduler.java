/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.core;

import static java.lang.Math.max;
import static java.lang.Math.min;

import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.utils.QueueInfo;
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
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
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

@SuppressWarnings("RedundantThrows")
public class MessageScheduler implements InitializingBean, DisposableBean, SmartLifecycle {
  private final Logger logger = LoggerFactory.getLogger(MessageScheduler.class);
  private static final long DEFAULT_SCRIPT_EXECUTION_TIME = 5000L;
  private static final long DEFAULT_DELAY = 5000L;
  private static final long MIN_DELAY = 10L;
  private static final int MAX_MESSAGE = 100;

  private final Object monitor = new Object();
  private final int poolSize;
  private volatile boolean running = false;
  private boolean scheduleTaskAtStartup;
  private Resource resource = new ClassPathResource("scripts/push-message.lua");
  private RedisScript<Long> redisScript;
  private MessageSchedulerListener messageSchedulerListener;
  private RedisTemplate<String, Long> redisTemplate;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, Boolean> queueRunningState;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask;
  private Map<String, String> channelNameToQueueName;
  private Map<String, String> queueNameToZsetName;
  private ThreadPoolTaskScheduler scheduler;

  @Autowired private RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Autowired private RedisMessageListenerContainer redisMessageListenerContainer;

  protected Logger getLogger() {
    return this.logger;
  }

  public MessageScheduler(
      RedisTemplate<String, Long> redisTemplate, int poolSize, boolean scheduleTaskAtStartup) {
    this.poolSize = poolSize;
    this.scheduleTaskAtStartup = scheduleTaskAtStartup;
    this.redisTemplate = redisTemplate;
  }

  @Override
  public boolean isRunning() {
    synchronized (monitor) {
      return this.running;
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
      this.running = false;
      monitor.notifyAll();
    }
    doStop();
  }

  private void doStop() {
    for (Map.Entry<String, Boolean> runningStateByQueue : this.queueRunningState.entrySet()) {
      if (runningStateByQueue.getValue()) {
        stopQueue(runningStateByQueue.getKey());
      }
    }
    waitForRunningQueuesToStop();
    this.queueNameToScheduledTask.clear();
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> queueRunningState : this.queueRunningState.entrySet()) {
      String queueName = queueRunningState.getKey();
      ScheduledTaskDetail scheduledTaskDetail = this.queueNameToScheduledTask.get(queueName);
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
        this.queueRunningState.containsKey(queueName),
        "Queue with name '" + queueName + "' does not exist");
    this.queueRunningState.put(queueName, false);
  }

  protected long getNextScheduleTime(long currentTime, Long value) {
    return currentTime + DEFAULT_DELAY;
  }

  private boolean isScheduleTaskAtStartup() {
    return scheduleTaskAtStartup;
  }

  @Override
  public void destroy() throws Exception {
    this.stop();
    if (scheduler != null) {
      scheduler.destroy();
    }
  }

  protected String getChannelName(String queueName) {
    return QueueInfo.getChannelName(queueName);
  }

  protected String getZsetName(String queueName) {
    return QueueInfo.getTimeQueueName(queueName);
  }

  protected String getThreadNamePrefix() {
    return "RQDelayed-";
  }

  protected boolean isQueueValid(ConsumerQueueDetail queueDetail) {
    return queueDetail.isDelayedQueue();
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
    redisScript = RedisScript.of(resource, Long.class);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    channelNameToQueueName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToZsetName = new ConcurrentHashMap<>(queueNames.size());
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

  private class MessageMoverTask implements Runnable {
    private final String queueName;
    private final String zsetName;

    String getQueueName() {
      return this.queueName;
    }

    MessageMoverTask(String queueName, String zsetName) {
      this.queueName = queueName;
      this.zsetName = zsetName;
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

  protected synchronized void schedule(
      String queueName, String zsetName, Long startTime, boolean forceSchedule) {
    if (!isQueueActive(queueName) || scheduler == null) {
      return;
    }
    ScheduledTaskDetail scheduledTaskDetail = queueNameToScheduledTask.get(queueName);
    long currentTime = System.currentTimeMillis();
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
    if (!completedOrCancelled && existingDelay < MIN_DELAY) {
      try {
        submittedTask.get(DEFAULT_SCRIPT_EXECUTION_TIME, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (ExecutionException | TimeoutException | CancellationException e) {
        //  no op
      }
    }
    // Run was succeeded or cancelled submit new one
    completedOrCancelled = submittedTask.isDone() || submittedTask.isCancelled();
    if (!completedOrCancelled) {
      submittedTask.cancel(true);
    }
    MessageMoverTask timerTask = new MessageMoverTask(queueName, zsetName);
    Future<?> future =
        scheduler.schedule(
            timerTask,
            Instant.ofEpochMilli(getNextScheduleTime(System.currentTimeMillis(), startTime)));
    scheduledTaskDetail.setFuture(future);
    scheduledTaskDetail.setStartTime(startTime);
  }

  private class MessageSchedulerListener implements MessageListener {
    @Override
    public void onMessage(Message message, byte[] pattern) {
      if (message.getBody().length == 0 || message.getChannel().length == 0) {
        return;
      }
      String body = new String(message.getBody());
      String channel = new String(message.getChannel());
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
        getLogger().error("Invalid data {} on channel {}", body, channel);
      }
    }
  }
}
