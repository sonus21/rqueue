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

package com.github.sonus21.rqueue.config;

import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Getter
@Setter
public class RqueueConfig {
  private final RedisConnectionFactory connectionFactory;
  private final boolean sharedConnection;
  private final int dbVersion;
  private final String id;

  public RqueueConfig(
      RedisConnectionFactory connectionFactory, boolean sharedConnection, int dbVersion) {
    this.connectionFactory = connectionFactory;
    this.sharedConnection = sharedConnection;
    this.dbVersion = dbVersion;
    this.id = UUID.randomUUID().toString();
  }

  @Value("${rqueue.version:2.1.0}")
  private String version;

  @Value("${rqueue.key.prefix:__rq::}")
  private String prefix;

  @Value("${rqueue.cluster.mode:true}")
  private boolean clusterMode;

  @Value("${rqueue.simple.queue.prefix:queue::}")
  private String simpleQueuePrefix;

  @Value("${rqueue.delayed.queue.prefix:d-queue::}")
  private String delayedQueuePrefix;

  @Value("${rqueue.delayed.queue.channel.prefix:d-channel::}")
  private String delayedQueueChannelPrefix;

  @Value("${rqueue.processing.queue.name.prefix:p-queue::}")
  private String processingQueuePrefix;

  @Value("${rqueue.processing.queue.channel.prefix:p-channel::}")
  private String processingQueueChannelPrefix;

  @Value("${rqueue.queues.key.suffix:queues}")
  private String queuesKeySuffix;

  @Value("${rqueue.lock.key.prefix:lock::}")
  private String lockKeyPrefix;

  @Value("${rqueue.queue.stat.key.prefix:q-stat::}")
  private String queueStatKeyPrefix;

  @Value("${rqueue.queue.config.key.prefix:q-config::}")
  private String queueConfigKeyPrefix;

  @Value("${rqueue.retry.per.poll:1}")
  private int retryPerPoll;

  @Value("${rqueue.add.default.queue.with.queue.level.priority:true}")
  private boolean addDefaultQueueWithQueueLevelPriority;

  @Value("${rqueue.default.queue.with.queue.level.priority:-1}")
  private int defaultQueueWithQueueLevelPriority;

  @Value("${rqueue.topics.key.suffix:topics}")
  private String topicsKeySuffix;

  @Value("${rqueue.topic.name.prefix:topic::}")
  private String topicNamePrefix;

  @Value("${rqueue.topic.configuration.key:t-config::}")
  private String topicConfigurationPrefix;

  @Value("${rqueue.topic.subscription.key:t-subscription::}")
  private String topicSubscriptionPrefix;

  @Value("${rqueue.event.channel:e-channel}")
  private String eventChannelSuffix;

  public String getQueuesKey() {
    return prefix + queuesKeySuffix;
  }

  public String getQueueName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + simpleQueuePrefix + getTaggedName(queueName);
    }
    return queueName;
  }

  public String getDelayedQueueName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + delayedQueuePrefix + getTaggedName(queueName);
    }
    return "rqueue-delay::" + queueName;
  }

  public String getDelayedQueueChannelName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + delayedQueueChannelPrefix + getTaggedName(queueName);
    }
    return "rqueue-channel::" + queueName;
  }

  public String getProcessingQueueName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + processingQueuePrefix + getTaggedName(queueName);
    }
    return "rqueue-processing::" + queueName;
  }

  public String getProcessingQueueChannelName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + processingQueueChannelPrefix + getTaggedName(queueName);
    }
    return "rqueue-processing-channel::" + queueName;
  }

  public String getLockKey(String key) {
    return prefix + lockKeyPrefix + key;
  }

  public String getQueueStatisticsKey(String name) {
    return prefix + queueStatKeyPrefix + name;
  }

  public String getQueueConfigKey(String name) {
    return prefix + queueConfigKeyPrefix + name;
  }

  public String getTopicsKey() {
    return getPrefix() + topicsKeySuffix;
  }

  public String getTopicName(String topic) {
    return getPrefix() + topicNamePrefix + topic;
  }

  public String getTopicSubscriptionKey(String topic) {
    return getPrefix() + topicSubscriptionPrefix + topic;
  }

  public String getTopicConfigurationKey(String topic) {
    return getPrefix() + topicConfigurationPrefix + topic;
  }

  public String getInternalEventChannel() {
    return getPrefix() + eventChannelSuffix;
  }

  private String getTaggedName(String queueName) {
    if (!clusterMode) {
      return queueName;
    }
    return "{" + queueName + "}";
  }
}
