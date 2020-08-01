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

import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.event.Level;

class WeightedPriorityPoller extends RqueueMessagePoller {
  private final Map<String, QueueThread> queueNameToThread;
  private final Map<String, QueueDetail> queueNameToDetail;
  private List<QueueDetail> queueDetailList;
  private int[] currentWeight;
  private int[] weight;
  private float[] probability;
  private int currentIndex = 0;

  WeightedPriorityPoller(
      String groupName,
      RqueueMessageListenerContainer container,
      final List<QueueDetail> queueDetails,
      final Map<String, QueueThread> queueNameToThread,
      PostProcessingHandler postProcessingHandler,
      int retryPerPoll) {
    super("Weighted-" + groupName, container, postProcessingHandler, retryPerPoll);
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
    log(Level.DEBUG, "reinitialized weight {}", null, (Object) currentWeight);
  }

  private int getQueueIndexToPoll() {
    int tmpIndex = (currentIndex + 1) % queues.size();
    while (tmpIndex != currentIndex) {
      String queue = queues.get(tmpIndex);
      if (currentWeight[tmpIndex] > 0 && isQueueActive(queue)) {
        currentWeight[tmpIndex] -= 1;
        currentIndex = tmpIndex;
        return currentIndex;
      }
      tmpIndex = (tmpIndex + 1) % queues.size();
    }
    return -1;
  }

  private int getQueueToPollOrWait() {
    int index = getQueueIndexToPoll();
    if (index == -1) {
      if (shouldExit()) {
        return -1;
      }
      index = -2;
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
        (Object) probability);
  }

  @Override
  void start() {
    initializeWeight();
    printDebugDetail();
    while (true) {
      try {
        int index = getQueueToPollOrWait();
        if (index == -1) {
          return;
        }
        if (index == -2) {
          TimeoutUtils.sleepLog(getPollingInterval(), false);
          reinitializeWeight();
        } else {
          String queue = queues.get(index);
          QueueThread queueThread = queueNameToThread.get(queue);
          QueueDetail queueDetail = queueNameToDetail.get(queue);
          poll(index, queue, queueDetail, queueThread);
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
  long getSemaphoreWaiTime() {
    return 25L;
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.POLL_FAILED) {
      TimeoutUtils.sleepLog(getBackOffTime(), false);
    } else {
      currentWeight[index] -= currentWeight[index] * (1 - probability[index]);
    }
  }
}
