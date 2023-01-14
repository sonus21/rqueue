/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Collections;
import java.util.List;
import org.slf4j.event.Level;
import org.springframework.messaging.MessageHeaders;

class DefaultRqueuePoller extends RqueueMessagePoller {

  private final QueueDetail queueDetail;
  private final QueueThreadPool queueThreadPool;

  DefaultRqueuePoller(
      QueueDetail queueDetail,
      QueueThreadPool queueThreadPool,
      RqueueBeanProvider rqueueBeanProvider,
      QueueStateMgr queueStateMgr,
      List<Middleware> middlewares,
      long pollingInterval,
      long backoffTime,
      PostProcessingHandler postProcessingHandler,
      MessageHeaders messageHeaders) {
    super(
        queueDetail.getName(),
        rqueueBeanProvider,
        queueStateMgr,
        middlewares,
        pollingInterval,
        backoffTime,
        postProcessingHandler,
        messageHeaders);
    this.queueDetail = queueDetail;
    this.queueThreadPool = queueThreadPool;
    this.queues = Collections.singletonList(queueDetail.getName());
  }

  @Override
  long getSemaphoreWaitTime() {
    return pollingInterval;
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.SEMAPHORE_UNAVAILABLE
        || deactivateType == DeactivateType.NO_MESSAGE) {
      TimeoutUtils.sleepLog(pollingInterval, false);
    } else if (deactivateType == DeactivateType.POLL_FAILED) {
      TimeoutUtils.sleepLog(backoffTime, false);
    }
  }

  void poll() {
    if (!hasAvailableThreads(queueDetail, queueThreadPool)) {
      log(Level.WARN, "No Threads are available sleeping {}Ms", null, pollingInterval);
      TimeoutUtils.sleepLog(pollingInterval, false);
    } else {
      super.poll(-1, queueDetail.getName(), queueDetail, queueThreadPool);
    }
  }

  @Override
  public void start() {
    log(Level.DEBUG, "Running Queue {}", null, queueDetail.getName());
    while (true) {
      try {
        if (eligibleForPolling(queueDetail.getName())) {
          poll();
        } else if (shouldExit()) {
          return;
        } else {
          deactivate(-1, queueDetail.getName(), DeactivateType.NO_MESSAGE);
        }
      } catch (Exception e) {
        log(Level.ERROR, "Error in poller", e);
        if (shouldExit()) {
          return;
        }
      }
    }
  }
}
