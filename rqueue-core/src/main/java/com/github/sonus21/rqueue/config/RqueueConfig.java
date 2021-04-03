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

package com.github.sonus21.rqueue.config;

import com.github.sonus21.rqueue.models.enums.RqueueMode;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.google.common.io.CharStreams;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Getter
@Setter
@RequiredArgsConstructor
@Configuration
public class RqueueConfig {

  private final RedisConnectionFactory connectionFactory;
  private final ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;
  private final boolean sharedConnection;
  private final int dbVersion;
  private static final String brokerId = UUID.randomUUID().toString();
  private static final AtomicLong counter = new AtomicLong(1);

  @Value("${rqueue.reactive.enabled:false}")
  private boolean reactiveEnabled;

  private String version = "";

  @Value("${rqueue.latest.version.check.enabled:true}")
  private boolean latestVersionCheckEnabled;

  @Value("${rqueue.key.prefix:__rq::}")
  private String prefix;

  @Value("${rqueue.del.prefix:del::")
  private String delPrefix;

  @Value("${rqueue.job.enabled:true}")
  private boolean jobEnabled;

  // 30 minutes
  @Value("${rqueue.job.durability.in-terminal-state:1800}")
  private long jobDurabilityInTerminalStateInSecond;

  @Value("${rqueue.job.key.prefix:job::}")
  private String jobKeyPrefix;

  @Value("${rqueue.jobs.collection.name.prefix:jobs::}")
  private String jobsCollectionNamePrefix;

  @Value("${rqueue.cluster.mode:true}")
  private boolean clusterMode;

  @Value("${rqueue.simple.queue.prefix:}")
  private String simpleQueuePrefix;

  @Value("${rqueue.delayed.queue.prefix:}")
  private String delayedQueuePrefix;

  @Value("${rqueue.delayed.queue.channel.prefix:}")
  private String delayedQueueChannelPrefix;

  @Value("${rqueue.processing.queue.name.prefix:}")
  private String processingQueuePrefix;

  @Value("${rqueue.processing.queue.channel.prefix:}")
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

  @Value("${rqueue.net.proxy.host:}")
  private String proxyHost;

  @Value("${rqueue.net.proxy.port:8000}")
  private Integer proxyPort;

  @Value("${rqueue.net.proxy.type:HTTP}")
  private Proxy.Type proxyType;

  // 7 days
  @Value("${rqueue.message.durability:10080}")
  private long messageDurabilityInMinute;

  // 30 minutes
  @Value("${rqueue.message.durability.in-terminal-state:1800}")
  private long messageDurabilityInTerminalStateInSecond;

  @Value("${rqueue.system.mode:BOTH}")
  private RqueueMode mode;

  public String getQueuesKey() {
    return prefix + queuesKeySuffix;
  }

  private String getSimpleQueueSuffix() {
    if (!StringUtils.isEmpty(simpleQueuePrefix)) {
      return simpleQueuePrefix;
    }
    if (dbVersion == 2) {
      return "queue::";
    }
    return "queue-v2::";
  }

  private String getDelayedQueueSuffix() {
    if (!StringUtils.isEmpty(delayedQueuePrefix)) {
      return delayedQueuePrefix;
    }
    if (dbVersion == 2) {
      return "d-queue::";
    }
    return "d-queue-v2::";
  }

  private String getDelayedQueueChannelSuffix() {
    if (!StringUtils.isEmpty(delayedQueueChannelPrefix)) {
      return delayedQueueChannelPrefix;
    }
    if (dbVersion == 2) {
      return "d-channel::";
    }
    return "d-channel-v2::";
  }

  private String getProcessingQueueSuffix() {
    if (!StringUtils.isEmpty(processingQueuePrefix)) {
      return processingQueuePrefix;
    }
    if (dbVersion == 2) {
      return "p-queue::";
    }
    return "p-queue-v2::";
  }

  private String getProcessingQueueChannelSuffix() {
    if (!StringUtils.isEmpty(processingQueueChannelPrefix)) {
      return processingQueueChannelPrefix;
    }
    if (dbVersion == 2) {
      return "p-channel::";
    }
    return "p-channel-v2::";
  }

  public String getQueueName(String queueName) {
    if (dbVersion == 1) {
      return queueName;
    }
    return prefix + getSimpleQueueSuffix() + getTaggedName(queueName);
  }

  public String getDelayedQueueName(String queueName) {
    if (dbVersion == 1) {
      return "rqueue-delay::" + queueName;
    }
    return prefix + getDelayedQueueSuffix() + getTaggedName(queueName);
  }

  public String getDelayedQueueChannelName(String queueName) {
    if (dbVersion == 1) {
      return "rqueue-channel::" + queueName;
    }
    return prefix + getDelayedQueueChannelSuffix() + getTaggedName(queueName);
  }

  public String getProcessingQueueName(String queueName) {
    if (dbVersion == 1) {
      return "rqueue-processing::" + queueName;
    }
    return prefix + getProcessingQueueSuffix() + getTaggedName(queueName);
  }

  public String getProcessingQueueChannelName(String queueName) {
    if (dbVersion == 1) {
      return "rqueue-processing-channel::" + queueName;
    }
    return prefix + getProcessingQueueChannelSuffix() + getTaggedName(queueName);
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

  private String getTaggedName(String queueName) {
    if (!clusterMode) {
      return queueName;
    }
    return "{" + queueName + "}";
  }

  public String getJobId() {
    return prefix + jobKeyPrefix + UUID.randomUUID().toString();
  }

  public String getJobsKey(String messageId) {
    return prefix + jobsCollectionNamePrefix + messageId;
  }

  public String getDelDataName() {
    return prefix
        + delPrefix
        + brokerId
        + Constants.REDIS_KEY_SEPARATOR
        + counter.incrementAndGet();
  }

  public Duration getJobDurabilityInTerminalState() {
    return Duration.ofSeconds(jobDurabilityInTerminalStateInSecond);
  }

  public static String getBrokerId() {
    return brokerId;
  }

  public String getLibVersion() {
    if (StringUtils.isEmpty(version)) {
      ClassPathResource resource =
          new ClassPathResource("META-INF/RQUEUE.MF", this.getClass().getClassLoader());
      try {
        InputStream inputStream = resource.getInputStream();
        String result =
            CharStreams.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        for (String line : result.split("\n")) {
          String[] words = line.trim().split(":");
          if (2 == words.length && words[0].equals("Version")) {
            version = words[1].split("-")[0];
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return version;
  }
}
