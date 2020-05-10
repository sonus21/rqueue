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
import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

@Slf4j
abstract class RqueueMessagePoller extends MessageContainerBase {
  List<String> queues;

  enum DeactivateType {
    POLL_FAILED,
    NO_MESSAGE,
    SEMAPHORE_EXCEPTION,
    SEMAPHORE_UNAVAILABLE,
  }

  private final TaskExecutionBackOff taskBackOff;
  private final int retryPerPoll;

  RqueueMessagePoller(
      String groupName,
      RqueueMessageListenerContainer container,
      TaskExecutionBackOff taskExecutionBackOff,
      int retryPerPoll) {
    super(log, groupName, container);
    taskBackOff = taskExecutionBackOff;
    this.retryPerPoll = retryPerPoll;
  }

  private RqueueMessage getMessage(QueueDetail queueDetail) {
    return getRqueueMessageTemplate()
        .pop(
            queueDetail.getQueueName(),
            queueDetail.getProcessingQueueName(),
            queueDetail.getProcessingQueueChannelName(),
            queueDetail.getVisibilityTimeout());
  }

  long getPollingInterval() {
    return Objects.requireNonNull(container.get()).getPollingInterval();
  }

  long getBackOffTime() {
    return Objects.requireNonNull(container.get()).getBackOffTime();
  }

  private void enqueue(QueueThread queueThread, QueueDetail queueDetail, RqueueMessage message) {
    queueThread
        .getTaskExecutor()
        .execute(
            new RqueueExecutor(
                message,
                queueDetail,
                queueThread.getSemaphore(),
                container,
                Objects.requireNonNull(container.get()).getRqueueMessageHandler(),
                retryPerPoll,
                taskBackOff));
  }

  boolean shouldExit() {
    for (String queueName : queues) {
      if (isQueueActive(queueName)) {
        return false;
      }
    }
    log(Level.INFO, "Shutting down all queues are inactive {}", null, queues);
    return true;
  }

  void poll(int index, String queue, QueueDetail queueDetail, QueueThread queueThread) {
    log(Level.DEBUG, "Polling queue {}", null, queue);
    Semaphore semaphore = queueThread.getSemaphore();
    boolean acquired;
    try {
      acquired = semaphore.tryAcquire(getSemaphoreWaiTime(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      log(Level.WARN, "Exception {}", e, e.getMessage());
      deactivate(index, queue, DeactivateType.SEMAPHORE_EXCEPTION);
      return;
    }
    if (!acquired) {
      deactivate(index, queue, DeactivateType.SEMAPHORE_UNAVAILABLE);
    } else if (isQueueActive(queue)) {
      try {
        RqueueMessage message = getMessage(queueDetail);
        log(Level.DEBUG, "Queue: {} Fetched Msg {}", null, queue, message);
        if (message != null) {
          enqueue(queueThread, queueDetail, message);
        } else {
          semaphore.release();
          deactivate(index, queue, DeactivateType.NO_MESSAGE);
        }
      } catch (Exception e) {
        semaphore.release();
        log(Level.WARN, "Listener failed for the queue {}", e, queue);
        deactivate(index, queue, DeactivateType.POLL_FAILED);
      }
    }
  }

  abstract long getSemaphoreWaiTime();

  abstract void deactivate(int index, String queue, DeactivateType deactivateType);
}
