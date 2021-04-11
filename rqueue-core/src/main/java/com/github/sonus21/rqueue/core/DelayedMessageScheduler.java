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

package com.github.sonus21.rqueue.core;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

@Slf4j
public class DelayedMessageScheduler extends MessageScheduler {

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected long getNextScheduleTime(String queueName, Long value) {
    long currentTime = System.currentTimeMillis();
    if (value == null) {
      return currentTime + rqueueSchedulerConfig.getDelayedMessageTimeIntervalInMilli();
    }
    if (value < currentTime) {
      return currentTime;
    }
    return currentTime + rqueueSchedulerConfig.getDelayedMessageTimeIntervalInMilli();
  }

  @Override
  protected String getChannelName(String queueName) {
    return EndpointRegistry.get(queueName).getDelayedQueueChannelName();
  }

  @Override
  protected String getZsetName(String queueName) {
    return EndpointRegistry.get(queueName).getDelayedQueueName();
  }

  @Override
  protected String getThreadNamePrefix() {
    return "delayedMessageScheduler-";
  }

  @Override
  protected int getThreadPoolSize() {
    return rqueueSchedulerConfig.getDelayedMessageThreadPoolSize();
  }

  @Override
  protected boolean isProcessingQueue(String queueName) {
    return false;
  }


}
