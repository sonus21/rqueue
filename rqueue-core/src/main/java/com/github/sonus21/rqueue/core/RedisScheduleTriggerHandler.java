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

import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.function.Function;

class RedisScheduleTriggerHandler {

  private final RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  private final RqueueSchedulerConfig rqueueSchedulerConfig;
  private final Logger logger;
  private final Function<String, Future<?>> scheduler;
  private final Function<String, String> channelNameProducer;
  private final List<String> queueNames;

  @VisibleForTesting
  Map<String, Long> queueNameToLastRunTime;
  @VisibleForTesting
  Map<String, Future<?>> queueNameToFuture;
  @VisibleForTesting
  Map<String, String> channelNameToQueueName;
  @VisibleForTesting
  MessageListener messageListener;

  RedisScheduleTriggerHandler(Logger logger,
      RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory,
      RqueueSchedulerConfig rqueueSchedulerConfig, List<String> queueNames,
      Function<String, Future<?>> scheduler, Function<String, String> channelNameProducer) {
    this.queueNames = queueNames;
    this.rqueueSchedulerConfig = rqueueSchedulerConfig;
    this.rqueueRedisListenerContainerFactory = rqueueRedisListenerContainerFactory;
    this.logger = logger;
    this.scheduler = scheduler;
    this.channelNameProducer = channelNameProducer;
  }

  void initialize() {
    this.messageListener = new MessageSchedulerListener();
    this.channelNameToQueueName = new HashMap<>(queueNames.size());
    this.queueNameToFuture = new ConcurrentHashMap<>(queueNames.size());
    this.queueNameToLastRunTime = new ConcurrentHashMap<>(queueNames.size());
  }

  void stop() {
    for (String queue : queueNames) {
      stopQueue(queue);
    }
  }

  void startQueue(String queueName) {
    queueNameToLastRunTime.put(queueName, 0L);
    subscribeToRedisTopic(queueName);
  }

  void stopQueue(String queueName) {
    Future<?> future = queueNameToFuture.get(queueName);
    ThreadUtils.waitForTermination(logger, future, rqueueSchedulerConfig.getTerminationWaitTime(),
        "An exception occurred while stopping scheduler queue '{}'", queueName);
    queueNameToLastRunTime.put(queueName, 0L);
    queueNameToFuture.remove(queueName);
    unsubscribeFromRedis(queueName);
  }

  private void unsubscribeFromRedis(String queueName) {
    String channelName = channelNameProducer.apply(queueName);
    logger.debug("Queue {} unsubscribe from channel {}", queueName, channelName);
    rqueueRedisListenerContainerFactory.removeMessageListener(messageListener,
        new ChannelTopic(channelName));
    channelNameToQueueName.put(channelName, queueName);
  }

  private void subscribeToRedisTopic(String queueName) {
    String channelName = channelNameProducer.apply(queueName);
    channelNameToQueueName.put(channelName, queueName);
    logger.debug("Queue {} subscribe to channel {}", queueName, channelName);
    rqueueRedisListenerContainerFactory.addMessageListener(messageListener,
        new ChannelTopic(channelName));
  }

  protected long getMinDelay() {
    return rqueueSchedulerConfig.minMessageMoveDelay();
  }


  /**
   * This MessageListener listen the event from Redis, its expected that the event should be only
   * raised when elements in the ZSET are lagging behind current time.
   */
  private class MessageSchedulerListener implements MessageListener {

    private void schedule(String queueName, long currentTime) {
      Future<?> future = queueNameToFuture.get(queueName);
      if (future == null || future.isCancelled() || future.isDone()) {
        queueNameToLastRunTime.put(queueName, currentTime);
        Future<?> newFuture = scheduler.apply(queueName);
        queueNameToFuture.put(queueName, newFuture);
      }
    }

    private void handleMessage(String queueName, long startTime) {
      long currentTime = System.currentTimeMillis();
      if (startTime > currentTime) {
        logger.warn("Received message body is not correct queue: {}, time: {}", queueName,
            startTime);
        return;
      }
      long lastRunTime = queueNameToLastRunTime.get(queueName);
      if (currentTime - lastRunTime < getMinDelay()) {
        return;
      }
      schedule(queueName, currentTime);
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
      if (message.getBody().length == 0 || message.getChannel().length == 0) {
        return;
      }
      String body = new String(message.getBody());
      String channel = new String(message.getChannel());
      logger.trace("Body: {} Channel: {}", body, channel);
      try {
        long startTime = Long.parseLong(body);
        String queueName = channelNameToQueueName.get(channel);
        if (queueName == null) {
          logger.warn("Unknown channel name {}", channel);
          return;
        }
        handleMessage(queueName, startTime);
      } catch (Exception e) {
        logger.error("Error occurred on a channel {}, body: {}", channel, body, e);
      }
    }
  }
}
