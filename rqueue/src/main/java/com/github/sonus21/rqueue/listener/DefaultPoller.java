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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DefaultPoller extends AbstractPoller implements Runnable {
  private final QueueDetail queueDetail;
  private final QueueThread queueThread;

  DefaultPoller(
      QueueThread queueThread,
      QueueDetail queueDetail,
      RqueueMessageListenerContainer container,
      TaskExecutionBackOff taskBackOff,
      int retryPerPoll) {
    super(container, taskBackOff, retryPerPoll);
    this.queueDetail = queueDetail;
    this.queueThread = queueThread;
  }

  @Override
  public void run() {
    log.debug("Running Queue {}", queueDetail.getName());
    while (isQueueActive(queueDetail.getName())) {
      boolean acquired = false;
      Semaphore semaphore = queueThread.getSemaphore();
      try {
        acquired = semaphore.tryAcquire(Constants.MIN_DELAY, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        log.warn("Exception {}", e.getMessage(), e);
      }
      if (acquired && isQueueActive(queueDetail.getName())) {
        try {
          RqueueMessage message = getMessage(queueDetail);
          log.debug("Queue: {} Fetched Msg {}", queueDetail.getName(), message);
          if (message != null) {
            enqueue(queueThread, queueDetail, message);
          } else {
            semaphore.release();
            TimeoutUtils.sleepLog(getPollingInterval(), false);
          }
        } catch (Exception e) {
          semaphore.release();
          log.warn(
              "Message listener failed for the queue {}, it will be retried in {} Ms",
              queueDetail.getName(),
              getBackOffTime(),
              e);
          TimeoutUtils.sleepLog(getBackOffTime(), false);
        }
      }
    }
  }
}
