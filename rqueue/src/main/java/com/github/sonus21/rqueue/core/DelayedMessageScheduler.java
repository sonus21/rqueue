/*
 * Copyright (c) 2019-2019, Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.utils.QueueInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

public class DelayedMessageScheduler extends MessageScheduler {
  private static final long DEFAULT_DELAY = 5000L;
  private final Logger logger = LoggerFactory.getLogger(DelayedMessageScheduler.class);

  public DelayedMessageScheduler(
      RedisTemplate<String, Long> redisTemplate, int poolSize, boolean scheduleTaskAtStartup) {
    super(redisTemplate, poolSize, scheduleTaskAtStartup);
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  protected long getNextScheduleTime(long currentTime, Long value) {
    if (value == null) {
      return currentTime + DEFAULT_DELAY;
    }
    if (value < currentTime) {
      return currentTime;
    }
    return currentTime + DEFAULT_DELAY;
  }

  @Override
  protected String getChannelName(String queueName) {
    return QueueInfo.getChannelName(queueName);
  }

  @Override
  protected String getZsetName(String queueName) {
    return QueueInfo.getTimeQueueName(queueName);
  }

  @Override
  protected String getThreadNamePrefix() {
    return "RQDelayed-";
  }

  @Override
  protected boolean isQueueValid(ConsumerQueueDetail queueDetail) {
    return queueDetail.isDelayedQueue();
  }
}
