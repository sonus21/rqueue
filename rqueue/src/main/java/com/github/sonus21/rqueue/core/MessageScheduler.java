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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

public class MessageScheduler {
  private static final Logger logger = LoggerFactory.getLogger(MessageScheduler.class);
  private static final long defaultDelay = 5000L;
  private static final long maxMessage = 100;
  private static final long maxDelay = 10;
  private static final int maxRecursionDepth = 100;
  private final Object monitor = new Object();
  private final int poolSize;
  private volatile boolean running = false;
  private boolean scheduleTaskAtStartup;
  private Resource resource = new ClassPathResource("scripts/push-message.lua");
  private RedisScript<Long> redisScript = RedisScript.of(resource, Long.class);
  private MessageSchedulerListener messageSchedulerListener = new MessageSchedulerListener();
  private LongMessageTemplate messageTemplate;
  private DefaultScriptExecutor<String> defaultScriptExecutor;
  private Map<String, ScheduledTaskDetail> queueNameToScheduledTask = new ConcurrentHashMap<>();
  private Map<String, String> channelNameToQueueName = new ConcurrentHashMap<>();
  private Map<String, String> queueNameToZsetName = new ConcurrentHashMap<>();

  private ScheduledExecutorService scheduledExecutorService;
  private RedisMessageListenerContainer rqueueRedisListenerContainer;

  public MessageScheduler(
      LongMessageTemplate messageTemplate,
      RedisMessageListenerContainer redisMessageListenerContainer,
      int poolSize,
      boolean scheduleTaskAtStartup) {
    this.messageTemplate = messageTemplate;
    this.rqueueRedisListenerContainer = redisMessageListenerContainer;
    this.poolSize = poolSize;
    this.scheduleTaskAtStartup = scheduleTaskAtStartup;
  }

  public void start(Collection<MessageQueue> messageQueues) {
    synchronized (monitor) {
      if (messageQueues.size() > 0 && !this.running) {
        this.running = true;
        defaultScriptExecutor = new DefaultScriptExecutor<>(messageTemplate.redisTemplate);
        scheduledExecutorService =
            Executors.newScheduledThreadPool(min(poolSize, messageQueues.size()));
        long scheduleAt = System.currentTimeMillis() + maxDelay;
        for (MessageQueue messageQueue : messageQueues) {
          if (isScheduleTaskAtStartup()) {
            ScheduledTaskDetail taskDetail =
                schedule(messageQueue.getQueueName(), messageQueue.getZsetName(), scheduleAt, true);
            queueNameToScheduledTask.put(messageQueue.getQueueName(), taskDetail);
          }
          rqueueRedisListenerContainer.addMessageListener(
              messageSchedulerListener, new ChannelTopic(messageQueue.getChannelName()));
          channelNameToQueueName.put(messageQueue.getChannelName(), messageQueue.getQueueName());
          queueNameToZsetName.put(messageQueue.getQueueName(), messageQueue.getZsetName());
        }
      }
    }
  }

  public boolean isRunning() {
    return this.running;
  }

  public void stop() {
    synchronized (monitor) {
      this.running = false;
    }
    if (scheduledExecutorService != null && !scheduledExecutorService.isShutdown()) {
      for (ScheduledTaskDetail queueDetail : queueNameToScheduledTask.values()) {
        queueDetail.getFuture().cancel(true);
      }
      scheduledExecutorService.shutdownNow();
      queueNameToScheduledTask.clear();
    }
  }

  protected long getNextScheduleTime(long currentTime, Long value) {
    return currentTime + defaultDelay;
  }

  public void setScheduleTaskAtStartup(boolean scheduleTaskAtStartup) {
    this.scheduleTaskAtStartup = scheduleTaskAtStartup;
  }

  private boolean isScheduleTaskAtStartup() {
    return scheduleTaskAtStartup;
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
        if (isRunning()) {
          recursionDepth += 1;
          long currentTime = System.currentTimeMillis();
          // schedule immediately
          if (recursionDepth == maxRecursionDepth) {
            schedule(queueName, zsetName, currentTime + maxDelay, false);
            return;
          }
          Long value =
              defaultScriptExecutor.execute(
                  redisScript,
                  Arrays.asList(queueName, zsetName),
                  System.currentTimeMillis(),
                  maxMessage);
          if (value != null) {
            if (value - currentTime < maxDelay) {
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
        if (existingDelay < maxDelay) {
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
