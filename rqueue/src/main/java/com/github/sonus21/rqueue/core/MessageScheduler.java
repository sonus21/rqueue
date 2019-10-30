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

import static java.lang.Long.max;
import static java.lang.Math.min;

import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.utils.QueueInfo;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
import org.springframework.util.Assert;

public class MessageScheduler implements InitializingBean, DisposableBean, SmartLifecycle {
  private static final Logger logger = LoggerFactory.getLogger(MessageScheduler.class);
  private static final long DEFAULT_DELAY = 5000L;
  private static final long MAX_MESSAGE = 100;
  private static final long MAX_DELAY = 10;
  private static final int MAX_RECURSION_DEPTH = 100;
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
  private ScheduledExecutorService scheduledExecutorService;

  @Autowired private RqueueMessageListenerContainer rqueueMessageListenerContainer;
  @Autowired private RedisMessageListenerContainer redisMessageListenerContainer;

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
    logger.info("Starting message mover scheduler");
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
    long scheduleAt = System.currentTimeMillis() + MAX_DELAY;
    if (isScheduleTaskAtStartup()) {
      ScheduledTaskDetail taskDetail =
          schedule(queueName, getZsetName(queueName), scheduleAt, true);
      queueNameToScheduledTask.put(queueName, taskDetail);
    }
    redisMessageListenerContainer.addMessageListener(
        messageSchedulerListener, new ChannelTopic(getChannelName(queueName)));
    channelNameToQueueName.put(getChannelName(queueName), queueName);
    queueNameToZsetName.put(queueName, getZsetName(queueName));
  }

  @Override
  public void stop() {
    logger.info("Stopping Message mover scheduler");
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
  }

  private void waitForRunningQueuesToStop() {
    for (Map.Entry<String, Boolean> queueRunningState : this.queueRunningState.entrySet()) {
      String queueName = queueRunningState.getKey();
      ScheduledTaskDetail scheduledTaskDetail = this.queueNameToScheduledTask.get(queueName);
      if (scheduledTaskDetail != null) {
        scheduledTaskDetail.getFuture().cancel(true);
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
    scheduledExecutorService.shutdown();
  }

  protected String getChannelName(String queueName) {
    return QueueInfo.getChannelName(queueName);
  }

  protected String getZsetName(String queueName) {
    return QueueInfo.getTimeQueueName(queueName);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    Set<String> queueNames = rqueueMessageListenerContainer.getRegisteredQueues().keySet();
    defaultScriptExecutor = new DefaultScriptExecutor<>(redisTemplate);
    messageSchedulerListener = new MessageSchedulerListener();
    redisScript = RedisScript.of(resource, Long.class);
    queueRunningState = new ConcurrentHashMap<>(queueNames.size());
    queueNameToScheduledTask = new ConcurrentHashMap<>(queueNames.size());
    channelNameToQueueName = new ConcurrentHashMap<>(queueNames.size());
    queueNameToZsetName = new ConcurrentHashMap<>(queueNames.size());
    scheduledExecutorService = Executors.newScheduledThreadPool(min(poolSize, queueNames.size()));
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

  private class MessageMoverTimerTask implements Runnable {
    private final String queueName;
    private final String zsetName;
    private int recursionDepth = 0;

    String getQueueName() {
      return this.queueName;
    }

    MessageMoverTimerTask(String queueName, String zsetName) {
      this.queueName = queueName;
      this.zsetName = zsetName;
    }

    @Override
    public String toString() {
      return "MessageMoverTimerTask(" + queueName + " )";
    }

    @Override
    public void run() {
      try {
        if (isQueueActive(queueName)) {
          recursionDepth += 1;
          long currentTime = System.currentTimeMillis();
          // schedule immediately
          if (recursionDepth == MAX_RECURSION_DEPTH) {
            schedule(queueName, zsetName, currentTime + MAX_DELAY, false);
            return;
          }
          Long value =
              defaultScriptExecutor.execute(
                  redisScript,
                  Arrays.asList(queueName, zsetName),
                  System.currentTimeMillis(),
                  MAX_MESSAGE);
          if (value != null) {
            if (value - currentTime < MAX_DELAY) {
              run();
            } else {
              schedule(queueName, zsetName, getNextScheduleTime(currentTime, value), false);
            }
          } else {
            schedule(queueName, zsetName, getNextScheduleTime(currentTime, value), false);
          }
        }
      } catch (RedisSystemException e) {
        // no op
      } catch (Exception e) {
        logger.warn("Task execution failed for queue: {}", queueName, e);
      }
    }
  }

  protected ScheduledTaskDetail schedule(
      String queueName, String zsetName, Long startTime, boolean cancelExistingOne) {
    ScheduledTaskDetail scheduledTaskDetail = queueNameToScheduledTask.get(queueName);
    long currentTime = System.currentTimeMillis();
    MessageScheduler.MessageMoverTimerTask timerTask =
        new MessageScheduler.MessageMoverTimerTask(queueName, zsetName);
    if (scheduledTaskDetail == null) {
      ScheduledFuture scheduledFuture =
          scheduledExecutorService.schedule(
              timerTask, max(1, startTime - currentTime), TimeUnit.MILLISECONDS);
      scheduledTaskDetail = new ScheduledTaskDetail(startTime, scheduledFuture);
      queueNameToScheduledTask.put(timerTask.getQueueName(), scheduledTaskDetail);
    } else {
      long requiredDelay = startTime - currentTime;
      if (cancelExistingOne) {
        long existingDelay = scheduledTaskDetail.getStartTime() - currentTime;
        if (existingDelay < MAX_DELAY) {
          return scheduledTaskDetail;
        }
        scheduledTaskDetail.getFuture().cancel(true);
      }
      scheduledTaskDetail.setFuture(
          scheduledExecutorService.schedule(
              timerTask, max(1, requiredDelay), TimeUnit.MILLISECONDS));
    }
    return scheduledTaskDetail;
  }

  private class MessageSchedulerListener implements MessageListener {
    @Override
    public void onMessage(Message message, byte[] pattern) {
      String body = new String(message.getBody());
      String channel = new String(message.getChannel());
      try {
        Long startTime = Long.parseLong(body);
        String queueName = channelNameToQueueName.get(channel);
        if (queueName == null) {
          logger.warn("Unknown channel name {}", channel);
          return;
        }
        String zsetName = queueNameToZsetName.get(queueName);
        if (zsetName == null) {
          logger.warn("Unknown zset name {}", queueName);
          return;
        }
        schedule(queueName, zsetName, startTime, true);
      } catch (NumberFormatException e) {
        logger.error("Invalid data {} on channel {}", body, channel);
      }
    }
  }
}
