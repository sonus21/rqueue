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

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

@Slf4j
public class ScheduledQueueMessageScheduler extends MessageScheduler {

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected long getNextScheduleTime(String queueName, long currentTime, Long value) {
    if (value == null) {
      return currentTime + rqueueSchedulerConfig.getScheduledMessageTimeIntervalInMilli();
    }
    if (value < currentTime) {
      return currentTime;
    }
    return currentTime + rqueueSchedulerConfig.getScheduledMessageTimeIntervalInMilli();
  }

  @Override
  protected String getChannelName(String queueName) {
    return EndpointRegistry.get(queueName).getScheduledQueueChannelName();
  }

  @Override
  protected String getZsetName(String queueName) {
    return EndpointRegistry.get(queueName).getScheduledQueueName();
  }

  @Override
  protected String getThreadNamePrefix() {
    return "scheduledQueueMsgScheduler-";
  }

  @Override
  protected int getThreadPoolSize() {
    return rqueueSchedulerConfig.getScheduledMessageThreadPoolSize();
  }

  @Override
  protected boolean isProcessingQueue() {
    return false;
  }
}
