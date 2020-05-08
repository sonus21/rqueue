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
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.task.AsyncTaskExecutor;

@Slf4j
class MessagePoller extends MessageContainerBase implements Runnable {
  private final QueueDetail queueDetail;
  private final Semaphore semaphore;
  private final AsyncTaskExecutor executor;
  private final TaskExecutionBackOff taskBackOff;
  private final int retryPerPoll;

  MessagePoller(
      QueueDetail queueDetail,
      RqueueMessageListenerContainer container,
      Semaphore semaphore,
      AsyncTaskExecutor executor,
      int retryPerPoll,
      TaskExecutionBackOff taskBackOff) {
    super(container);
    this.queueDetail = queueDetail;
    this.semaphore = semaphore;
    this.executor = executor;
    this.retryPerPoll = retryPerPoll;
    this.taskBackOff = taskBackOff;
  }

  private RqueueMessage getMessage() {
    return getRqueueMessageTemplate()
        .pop(
            queueDetail.getQueueName(),
            queueDetail.getProcessingQueueName(),
            queueDetail.getProcessingQueueChannelName(),
            queueDetail.getVisibilityTimeout());
  }

  private void enqueueTask(RqueueMessage message) {
    executor.execute(
        new MessageExecutor(
            message,
            queueDetail,
            semaphore,
            container,
            Objects.requireNonNull(container.get()).getRqueueMessageHandler(),
            retryPerPoll,
            taskBackOff));
  }

  @Override
  public void run() {
    log.debug("Running Queue {}", queueDetail.getName());
    while (isQueueActive(queueDetail.getName())) {
      boolean acquired = false;
      try {
        acquired = semaphore.tryAcquire(Constants.MIN_DELAY, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        log.warn("Exception {}", e.getMessage(), e);
      }
      if (acquired && isQueueActive(queueDetail.getName())) {
        try {
          RqueueMessage message = getMessage();
          log.debug("Queue: {} Fetched Msg {}", queueDetail.getName(), message);
          if (message != null) {
            enqueueTask(message);
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

  private long getPollingInterval() {
    return Objects.requireNonNull(container.get()).getPollingInterval();
  }

  private long getBackOffTime() {
    return Objects.requireNonNull(container.get()).getBackOffTime();
  }
}
