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
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.event.Level;
import org.springframework.messaging.MessageHeaders;

class WeightedPriorityPoller extends RqueueMessagePoller {

  private static final int ALL_QUEUES_ARE_INELIGIBLE = -1;
  private static final int ALL_QUEUES_ARE_INACTIVE = -2;
  private final Map<String, QueueThreadPool> queueNameToThread;
  private final Map<String, QueueDetail> queueNameToDetail;
  private final List<QueueDetail> queueDetailList;
  private int[] currentWeight;
  private int[] weight;
  private float[] probability;
  private int currentIndex = 0;

  WeightedPriorityPoller(
      String groupName,
      final List<QueueDetail> queueDetails,
      final Map<String, QueueThreadPool> queueNameToThread,
      RqueueBeanProvider rqueueBeanProvider,
      QueueStateMgr queueStateMgr,
      List<Middleware> middlewares,
      long pollingInterval,
      long backoffTime,
      PostProcessingHandler postProcessingHandler,
      MessageHeaders messageHeaders) {
    super(
        "Weighted-" + groupName,
        rqueueBeanProvider,
        queueStateMgr,
        middlewares,
        pollingInterval,
        backoffTime,
        postProcessingHandler,
        messageHeaders);
    this.queueDetailList = queueDetails;
    this.queues = queueDetails.stream().map(QueueDetail::getName).collect(Collectors.toList());
    this.queueNameToDetail =
        queueDetails.stream().collect(Collectors.toMap(QueueDetail::getName, Function.identity()));
    this.queueNameToThread = queueNameToThread;
  }

  private void initializeWeight() {
    currentWeight = new int[queues.size()];
    weight = new int[queues.size()];
    probability = new float[queues.size()];
    float total = 0;
    for (int i = 0; i < queues.size(); i++) {
      QueueDetail queueDetail = this.queueDetailList.get(i);
      currentWeight[i] = queueDetail.getPriority().get(Constants.DEFAULT_PRIORITY_KEY);
      weight[i] = currentWeight[i];
      total += weight[i];
    }
    if (total == 0) {
      throw new IllegalStateException("Total priority is zero!!");
    }
    for (int i = 0; i < weight.length; i++) {
      probability[i] = weight[i] / total;
    }
  }

  private void reinitializeWeight() {
    currentIndex = 0;
    System.arraycopy(weight, 0, currentWeight, 0, weight.length);
    log(Level.DEBUG, "reinitialized weight {}", null, currentWeight);
  }

  private int getQueueIndexToPoll() {
    if (queues.size() > 1) {
      int tmpIndex = (currentIndex + 1) % queues.size();
      while (tmpIndex != currentIndex) {
        String queue = queues.get(tmpIndex);
        if (currentWeight[tmpIndex] > 0 && eligibleForPolling(queue)) {
          currentWeight[tmpIndex] -= 1;
          currentIndex = tmpIndex;
          return currentIndex;
        }
        tmpIndex = (tmpIndex + 1) % queues.size();
      }
    } else {
      String queue = queues.get(currentIndex);
      if (currentWeight[currentIndex] > 0 && eligibleForPolling(queue)) {
        currentWeight[currentIndex] -= 1;
        return currentIndex;
      }
    }
    return ALL_QUEUES_ARE_INELIGIBLE;
  }

  private int getQueueToPollOrWait() {
    int index = getQueueIndexToPoll();
    if (index == ALL_QUEUES_ARE_INELIGIBLE) {
      if (shouldExit()) {
        return ALL_QUEUES_ARE_INACTIVE;
      }
      index = ALL_QUEUES_ARE_INELIGIBLE;
    }
    if (isDebugEnabled()) {
      if (index >= 0) {
        log(Level.DEBUG, "Polling queue: {}", null, queues.get(index));
      } else {
        log(Level.DEBUG, "No queue to poll", null);
      }
    }
    return index;
  }

  private void printDebugDetail() {
    if (!isDebugEnabled()) {
      return;
    }
    List<String> weightStr =
        Arrays.stream(currentWeight).mapToObj(String::valueOf).collect(Collectors.toList());
    log(
        Level.DEBUG,
        "Running Queues: {} Weight: {} Average: {}",
        null,
        queues,
        weightStr,
        probability);
  }

  @Override
  public void start() {
    initializeWeight();
    printDebugDetail();
    while (true) {
      try {
        int index = getQueueToPollOrWait();
        if (index == ALL_QUEUES_ARE_INACTIVE) {
          return;
        }
        if (index == ALL_QUEUES_ARE_INELIGIBLE) {
          TimeoutUtils.sleepLog(pollingInterval, false);
          reinitializeWeight();
        } else {
          String queue = queues.get(index);
          QueueThreadPool queueThreadPool = queueNameToThread.get(queue);
          QueueDetail queueDetail = queueNameToDetail.get(queue);
          poll(index, queue, queueDetail, queueThreadPool);
        }
      } catch (Exception e) {
        log(Level.ERROR, "Error in poller", e);
        if (shouldExit()) {
          return;
        }
      }
    }
  }

  @Override
  long getSemaphoreWaitTime() {
    return 25L;
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.POLL_FAILED) {
      TimeoutUtils.sleepLog(backoffTime, false);
    } else {
      currentWeight[index] -= currentWeight[index] * (1 - probability[index]);
    }
  }
}
