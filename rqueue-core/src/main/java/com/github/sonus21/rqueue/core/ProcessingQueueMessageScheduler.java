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

import static java.lang.Long.max;

import com.github.sonus21.rqueue.listener.QueueDetail;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import com.github.sonus21.rqueue.utils.Constants;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;

@Slf4j
public class ProcessingQueueMessageScheduler extends MessageScheduler {

  private Map<String, Long> queueNameToDelay;

  @Override
  protected void initialize() {
    super.initialize();
    List<QueueDetail> queueDetails = EndpointRegistry.getActiveQueueDetails();
    this.queueNameToDelay = new ConcurrentHashMap<>(queueDetails.size());
    for (QueueDetail queueDetail : queueDetails) {
      this.queueNameToDelay.put(queueDetail.getName(), queueDetail.getVisibilityTimeout());
    }
  }

  @Override
  protected Logger getLogger() {
    return log;
  }

  @Override
  protected String getChannelName(String queueName) {
    return EndpointRegistry.get(queueName).getProcessingQueueChannelName();
  }

  @Override
  protected String getZsetName(String queueName) {
    return EndpointRegistry.get(queueName).getProcessingQueueName();
  }

  @Override
  protected int getThreadPoolSize() {
    return rqueueSchedulerConfig.getProcessingMessageThreadPoolSize();
  }

  @Override
  protected boolean isProcessingQueue() {
    return true;
  }

  @Override
  protected String getThreadNamePrefix() {
    return "processingQueueMsgScheduler-";
  }

  @Override
  protected long getNextScheduleTime(String queueName, long currentTime, Long value) {
    if (value == null) {
      long delay = queueNameToDelay.get(queueName);
      return currentTime + delay;
    }
    return max(currentTime, value);
  }
}
