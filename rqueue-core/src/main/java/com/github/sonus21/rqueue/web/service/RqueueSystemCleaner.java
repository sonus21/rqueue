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

package com.github.sonus21.rqueue.web.service;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.RetryableRunnable;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class RqueueSystemCleaner implements ApplicationListener<RqueueBootstrapEvent> {
  private ScheduledExecutorService executorService;
  private final RqueueBeanProvider rqueueBeanProvider;

  @Autowired
  public RqueueSystemCleaner(RqueueBeanProvider rqueueBeanProvider) {
    this.rqueueBeanProvider = rqueueBeanProvider;
  }

  private class CleanCompletedJobs extends RetryableRunnable<Object> {
    private final String queueName;
    private final String completedQueueName;

    protected CleanCompletedJobs(String queueName, String completedQueueName) {
      super(log, "");
      this.queueName = queueName;
      this.completedQueueName = completedQueueName;
    }

    @Override
    public void start() {
      QueueConfig queueConfig =
          rqueueBeanProvider.getRqueueSystemConfigDao().getConfigByName(queueName, true);
      if (queueConfig == null) {
        log.error("Queue config does not exist {}", queueName);
        return;
      }
      if (queueConfig.isPaused()) {
        log.debug("Queue {} is paused", queueName);
        return;
      }

      long endTime =
          System.currentTimeMillis()
              - rqueueBeanProvider
                  .getRqueueConfig()
                  .messageDurabilityInTerminalStateInMillisecond();
      log.debug("Deleting completed messages for queue: {}, before: {}", endTime, queueName);
      rqueueBeanProvider
          .getRqueueMessageMetadataService()
          .deleteQueueMessages(completedQueueName, endTime);
    }
  }

  @Override
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    if (executorService != null) {
      executorService.shutdown();
    }
    if (event.isStartup()
        && rqueueBeanProvider.getRqueueConfig().messageInTerminalStateShouldBeStored()) {
      executorService = Executors.newSingleThreadScheduledExecutor();
      List<QueueConfig> queueConfigList =
          rqueueBeanProvider
              .getRqueueSystemConfigDao()
              .getConfigByNames(EndpointRegistry.getActiveQueues());
      int i = 0;
      for (QueueConfig queueConfig : queueConfigList) {
        CleanCompletedJobs completedJobs =
            new CleanCompletedJobs(queueConfig.getName(), queueConfig.getCompletedQueueName());
        executorService.scheduleAtFixedRate(
            completedJobs, i, 5 * Constants.ONE_MILLI, TimeUnit.MILLISECONDS);
      }
    }
  }
}
