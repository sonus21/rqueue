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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class WeightedPriorityPoller extends AbstractPoller implements Runnable {
  private final List<String> queues;
  private int[] weight;
  private List<QueueDetail> queueDetailList;
  private final Map<String, QueueThread> queueNameToThread;
  private final Map<String, QueueDetail> queueNameToDetail;
  private int currentIndex = 0;

  WeightedPriorityPoller(
      RqueueMessageListenerContainer container,
      final List<QueueDetail> queueDetails,
      final Map<String, QueueThread> queueNameToThread,
      TaskExecutionBackOff taskExecutionBackOff,
      int retryPerPoll) {
    super(container, taskExecutionBackOff, retryPerPoll);
    this.queueDetailList = queueDetails;
    this.queues = queueDetails.stream().map(QueueDetail::getName).collect(Collectors.toList());
    this.queueNameToDetail =
        queueDetails.stream().collect(Collectors.toMap(QueueDetail::getName, Function.identity()));
    this.queueNameToThread = queueNameToThread;
    initializeWeight();
  }

  private void initializeWeight() {
    weight = new int[queues.size()];
    reinitializeWeight();
  }

  private void reinitializeWeight() {
    for (int i = 0; i < queues.size(); i++) {
      QueueDetail queueDetail = this.queueDetailList.get(i);
      weight[i] = queueDetail.getPriority().get(Constants.DEFAULT_PRIORITY_KEY);
    }
  }

  private void reinitializeWeightIfRequired() {
    int zeroWeight = 0;
    for (int i = 0; i < queues.size(); i++) {
      if (weight[i] == 0) {
        zeroWeight += 1;
      }
    }
    if (zeroWeight == queues.size()) {
      reinitializeWeight();
    }
  }

  private int getQueueIndexToPoll() {
    int tmpIndex = (currentIndex + 1) % queues.size();
    while (tmpIndex != currentIndex) {
      String queue = queues.get(tmpIndex);
      if (isQueueActive(queue) && weight[tmpIndex] > 0) {
        weight[tmpIndex] -= 1;
        currentIndex = tmpIndex;
        return currentIndex;
      }
      tmpIndex = (tmpIndex + 1) % queues.size();
    }
    return -1;
  }

  private int getQueueToPoll() {
    int index = getQueueIndexToPoll();
    if (index == -1) {
      reinitializeWeightIfRequired();
      return getQueueIndexToPoll();
    }
    return index;
  }

  private boolean shouldExit() {
    for (String queueName : queues) {
      if (isQueueActive(queueName)) {
        return false;
      }
    }
    return true;
  }

  private int getQueueToPollOrWait() {
    int index = getQueueToPoll();
    if (index == -1) {
      if (shouldExit()) {
        log.info("Exiting all queues are inactive {}", queues);
        return -1;
      } else {
        TimeoutUtils.sleepLog(getPollingInterval(), false);
      }
    }
    return index;
  }

  @Override
  public void run() {
    log.debug("Running");
    while (true) {
      int index = getQueueToPollOrWait();
      if (index == -1) {
        break;
      }
      String queueToPoll = queues.get(index);
      QueueThread queueThread = queueNameToThread.get(queueToPoll);
      QueueDetail queueDetail = queueNameToDetail.get(queueToPoll);
      Semaphore semaphore = queueThread.getSemaphore();
      boolean acquired = false;
      try {
        acquired = semaphore.tryAcquire(10L, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        deactivate(index);
        log.warn("Exception {}", e.getMessage(), e);
      }
      if (!acquired) {
        deactivate(index);
      } else if (isQueueActive(queueToPoll)) {
        try {
          RqueueMessage message = getMessage(queueDetail);
          log.debug("Queue: {} Fetched Msg {}", queueDetail.getName(), message);
          if (message != null) {
            enqueue(queueThread, queueDetail, message);
          } else {
            semaphore.release();
            deactivate(index);
          }
        } catch (Exception e) {
          semaphore.release();
          deactivate(index);
          log.warn("Message listener failed for the queue {}", queueDetail.getName(), e);
        }
      }
    }
  }

  private void deactivate(int index) {
    weight[index] = 0;
  }
}
