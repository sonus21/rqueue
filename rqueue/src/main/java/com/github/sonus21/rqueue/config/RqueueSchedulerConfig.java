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
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Configuration
@Getter
@Setter
public class RqueueSchedulerConfig {

  /**
   * Whether scheduling feature is enabled or not. This can be used to control different servers
   * where on a server only scheduler is running, while on other only workers are running.
   */
  @Value("${rqueue.scheduler.enabled:true}")
  private boolean enabled;

  /**
   * This is used to control whether same redis {@link RedisMessageListenerContainer} would be used
   * or new one should be created, this must be set to true when using two different connection for
   * Rqueue and application Redis.
   */
  @Value("${rqueue.scheduler.listener.shared:true}")
  private boolean listenerShared;

  /**
   * This is used to control message scheduler auto start feature, if it's disabled then messages
   * are moved only when a message is received from Redis PUB/SUB channel.
   */
  @Value("${rqueue.scheduler.auto.start:true}")
  private boolean autoStart;

  /**
   * This is used to control message scheduler redis pub/sub interaction, this can be used to
   * completely disable the redis PUB/SUB interaction
   */
  @Value("${rqueue.scheduler.redis.enabled:true}")
  private boolean redisEnabled;

  // Number of threads used to process delayed queue messages
  @Value("${rqueue.scheduler.delayed.message.thread.pool.size:5}")
  private int delayedMessagePoolSize;

  //  Number of threads used to process processing queue messages
  @Value("${rqueue.scheduler.processing.message.thread.pool.size:1}")
  private int processingMessagePoolSize;
}
