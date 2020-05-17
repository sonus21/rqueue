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

import static com.github.sonus21.rqueue.utils.Constants.BLANK;

import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.ThreadUtils.QueueThread;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.event.Level;

class StrictPriorityPoller extends RqueueMessagePoller {
  private final Map<String, QueueDetail> queueNameToDetail;
  private final Map<String, QueueThread> queueNameToThread;
  private final Map<String, Long> queueDeactivationTime = new HashMap<>();
  private Map<String, Long> lastFetchedTime = new HashMap<>();

  StrictPriorityPoller(
      String groupName,
      RqueueMessageListenerContainer container,
      final List<QueueDetail> queueDetails,
      final Map<String, QueueThread> queueNameToThread,
      TaskExecutionBackOff taskExecutionBackOff,
      int retryPerPoll) {
    super("Strict-" + groupName, container, taskExecutionBackOff, retryPerPoll);
    List<QueueDetail> queueDetailList = new ArrayList<>(queueDetails);
    queueDetailList.sort(
        (o1, o2) ->
            o2.getPriority().get(Constants.DEFAULT_PRIORITY_KEY)
                - o1.getPriority().get(Constants.DEFAULT_PRIORITY_KEY));
    this.queues = queueDetailList.stream().map(QueueDetail::getName).collect(Collectors.toList());
    queues.forEach(queue -> this.lastFetchedTime.put(queue, 0L));
    this.queueNameToDetail =
        queueDetailList.stream()
            .collect(Collectors.toMap(QueueDetail::getName, Function.identity()));
    this.queueNameToThread = queueNameToThread;
  }

  private String getQueueToPoll() {
    long now = System.currentTimeMillis();
    for (String queue : queues) {
      if (isQueueActive(queue)) {
        if (now - lastFetchedTime.get(queue) > Constants.MILLIS_IN_A_MINUTE) {
          return queue;
        }
      }
    }
    for (String queue : queues) {
      if (isQueueActive(queue)) {
        Long deactivationTime = queueDeactivationTime.get(queue);
        if (deactivationTime == null) {
          return queue;
        }
        if (now - deactivationTime > getPollingInterval()) {
          return queue;
        }
      }
    }
    return null;
  }

  private String getQueueToPollOrWait() {
    String queueToPoll = getQueueToPoll();
    if (queueToPoll == null) {
      if (shouldExit()) {
        return null;
      }
      queueToPoll = "";
    }
    log(Level.DEBUG, "Queue to be poll : {}", null, queueToPoll);
    return queueToPoll;
  }

  @Override
  void start() {
    log(Level.DEBUG, "Running, Ordered Queues: {}", null, queues);
    while (true) {
      try {
        String queue = getQueueToPollOrWait();
        if (queue == null) {
          return;
        }
        if (queue.equals(BLANK)) {
          TimeoutUtils.sleepLog(getPollingInterval(), false);
        } else {
          lastFetchedTime.put(queue, System.currentTimeMillis());
          QueueThread queueThread = queueNameToThread.get(queue);
          QueueDetail queueDetail = queueNameToDetail.get(queue);
          poll(-1, queue, queueDetail, queueThread);
        }
      } catch (Exception e) {
        log(Level.ERROR, "Exception in the poller {}", e, e.getMessage());
        if (shouldExit()) {
          return;
        }
      }
    }
  }

  @Override
  long getSemaphoreWaiTime() {
    return 20L;
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.POLL_FAILED) {
      TimeoutUtils.sleepLog(getBackOffTime(), false);
    } else {
      queueDeactivationTime.put(queue, System.currentTimeMillis());
    }
  }
}
