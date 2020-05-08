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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class StrictPriorityPoller extends AbstractPoller implements Runnable {
  private final Map<String, QueueDetail> queueNameToDetail;
  private final List<String> queueByPriority;
  private Map<String, Long> lastFetchedTime = new HashMap<>();
  private final Map<String, QueueThread> queueNameToThread;
  private final Map<String, Long> queueDeactivationTime = new HashMap<>();

  StrictPriorityPoller(
      RqueueMessageListenerContainer container,
      final List<QueueDetail> queueDetails,
      final Map<String, QueueThread> queueNameToThread,
      TaskExecutionBackOff taskExecutionBackOff,
      int retryPerPoll) {
    super(container, taskExecutionBackOff, retryPerPoll);
    List<QueueDetail> queueDetailList = new ArrayList<>(queueDetails);
    queueDetailList.sort(
        (o1, o2) ->
            o2.getPriority().get(Constants.DEFAULT_PRIORITY_KEY)
                - o1.getPriority().get(Constants.DEFAULT_PRIORITY_KEY));
    this.queueByPriority =
        queueDetailList.stream().map(QueueDetail::getName).collect(Collectors.toList());
    for (String queue : queueByPriority) {
      this.lastFetchedTime.put(queue, System.currentTimeMillis());
    }
    this.queueNameToDetail =
        queueDetailList.stream()
            .collect(Collectors.toMap(QueueDetail::getName, Function.identity()));
    this.queueNameToThread = queueNameToThread;
  }

  private String getQueueToPoll() {
    long now = System.currentTimeMillis();
    for (String queue : queueByPriority) {
      if (isQueueActive(queue)) {
        if (lastFetchedTime.get(queue) - now > Constants.MILLIS_IN_A_MINUTE) {
          return queue;
        }
      }
    }
    for (String queue : queueByPriority) {
      if (isQueueActive(queue)) {
        Long lastDeactivationTime = queueDeactivationTime.get(queue);
        if (lastDeactivationTime == null) {
          return queue;
        }
        if (lastDeactivationTime - now > Constants.MILLIS_IN_A_MINUTE) {
          return queue;
        }
      }
    }
    return null;
  }

  private void deactivate(String queueName) {
    queueDeactivationTime.put(queueName, System.currentTimeMillis());
  }

  private boolean shouldExit() {
    for (String queueName : queueByPriority) {
      if (isQueueActive(queueName)) {
        return false;
      }
    }
    return true;
  }

  private String getQueueToPollOrWait() {
    String queueToPoll = getQueueToPoll();
    if (queueToPoll == null) {
      if (shouldExit()) {
        log.info("Exiting all queues are inactive {}", queueByPriority);
        return null;
      } else {
        TimeoutUtils.sleepLog(getPollingInterval(), false);
      }
    }
    return queueToPoll;
  }

  @Override
  public void run() {
    log.debug("Running");
    while (true) {
      String queueToPoll = getQueueToPollOrWait();
      if (queueToPoll == null) {
        break;
      }
      lastFetchedTime.put(queueToPoll, System.currentTimeMillis());
      QueueThread queueThread = queueNameToThread.get(queueToPoll);
      QueueDetail queueDetail = queueNameToDetail.get(queueToPoll);
      Semaphore semaphore = queueThread.getSemaphore();
      boolean acquired = false;
      try {
        acquired = semaphore.tryAcquire(10L, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        deactivate(queueToPoll);
        log.warn("Exception {}", e.getMessage(), e);
      }
      if (!acquired) {
        deactivate(queueToPoll);
      } else if (isQueueActive(queueToPoll)) {
        try {
          RqueueMessage message = getMessage(queueDetail);
          log.debug("Queue: {} Fetched Msg {}", queueDetail.getName(), message);
          if (message != null) {
            enqueue(queueThread, queueDetail, message);
          } else {
            semaphore.release();
            deactivate(queueToPoll);
          }
        } catch (Exception e) {
          semaphore.release();
          deactivate(queueToPoll);
          log.warn("Message listener failed for the queue {}", queueDetail.getName(), e);
        }
      }
    }
  }
}
