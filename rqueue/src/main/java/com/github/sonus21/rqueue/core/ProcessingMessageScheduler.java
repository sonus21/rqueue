/*
 * Copyright 2019 Sonu Kumar
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

import static java.lang.Long.max;

import com.github.sonus21.rqueue.listener.ConsumerQueueDetail;
import com.github.sonus21.rqueue.utils.QueueInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;

public class ProcessingMessageScheduler extends MessageScheduler {
  private final Logger logger = LoggerFactory.getLogger(ProcessingMessageScheduler.class);

  public ProcessingMessageScheduler(
      RedisTemplate<String, Long> redisTemplate, int poolSize, boolean scheduleTaskAtStartup) {
    super(redisTemplate, poolSize, scheduleTaskAtStartup);
  }

  @Override
  protected Logger getLogger() {
    return logger;
  }

  @Override
  protected String getChannelName(String queueName) {
    return QueueInfo.getProcessingQueueChannelName(queueName);
  }

  @Override
  protected String getZsetName(String queueName) {
    return QueueInfo.getProcessingQueueName(queueName);
  }

  @Override
  protected boolean isQueueValid(ConsumerQueueDetail queueDetail) {
    return true;
  }

  @Override
  protected String getThreadNamePrefix() {
    return "RQProcessing-";
  }

  @Override
  protected long getNextScheduleTime(long currentTime, Long value) {
    if (value == null) {
      return QueueInfo.getMessageReEnqueueTime(currentTime);
    }
    return max(currentTime, value);
  }
}
