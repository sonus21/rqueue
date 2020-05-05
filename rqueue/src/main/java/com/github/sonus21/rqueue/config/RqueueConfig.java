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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;

@Getter
@Setter
@RequiredArgsConstructor
@Configuration
public class RqueueConfig {
  private final RedisConnectionFactory connectionFactory;
  private final boolean sharedConnection;
  private final int dbVersion;

  @Value("${rqueue.version:2.0.0-RELEASE}")
  private String version;

  @Value("${rqueue.key.prefix:__rq::}")
  private String prefix;

  @Value("${rqueue.cluster.mode:true}")
  private boolean clusterMode;

  @Value("${rqueue.simple.queue.name.prefix:queue::}")
  private String simpleQueueNamePrefix;

  @Value("${rqueue.delayed.queue.name.prefix:d-queue::}")
  private String delayedQueueNamePrefix;

  @Value("${rqueue.delayed.queue.channel.name.prefix:d-channel::}")
  private String delayedQueueChannelNamePrefix;

  @Value("${rqueue.processing.queue.name.prefix:p-queue::}")
  private String processingQueueNamePrefix;

  @Value("${rqueue.processing.queue.channel.name.prefix:p-channel::}")
  private String processingQueueChannelNamePrefix;

  public String getQueueName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + simpleQueueNamePrefix + getTaggedName(queueName);
    }
    return queueName;
  }

  public String getDelayedQueueName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + delayedQueueNamePrefix + getTaggedName(queueName);
    }
    return "rqueue-delay::" + queueName;
  }

  public String getDelayedQueueChannelName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + delayedQueueChannelNamePrefix + getTaggedName(queueName);
    }
    return "rqueue-channel::" + queueName;
  }

  public String getProcessingQueueName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + processingQueueNamePrefix + getTaggedName(queueName);
    }
    return "rqueue-processing::" + queueName;
  }

  public String getProcessingQueueChannelName(String queueName) {
    if (dbVersion >= 2) {
      return prefix + processingQueueChannelNamePrefix + getTaggedName(queueName);
    }
    return "rqueue-processing-channel::" + queueName;
  }

  private String getTaggedName(String queueName) {
    if (!clusterMode) {
      return queueName;
    }
    return "{" + queueName + "}";
  }
}
