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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.event.Level;
import org.springframework.messaging.MessageHeaders;

class StrictPriorityPoller extends RqueueMessagePoller {

  private static final String ALL_QUEUES_ARE_INELIGIBLE = "\uD83D\uDE1F";
  private static final String ALL_QUEUES_ARE_INACTIVE = "\uD83D\uDC4B";
  private final Map<String, QueueDetail> queueNameToDetail;
  private final Map<String, QueueThreadPool> queueNameToThread;
  private final Map<String, Long> queueDeactivationTime = new HashMap<>();
  private final Map<String, Long> lastFetchedTime = new HashMap<>();

  StrictPriorityPoller(
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
        "Strict-" + groupName,
        rqueueBeanProvider,
        queueStateMgr,
        middlewares,
        pollingInterval,
        backoffTime,
        postProcessingHandler,
        messageHeaders);
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
    // starvation
    for (String queue : queues) {
      if (eligibleForPolling(queue)) {
        if (now - lastFetchedTime.get(queue) > Constants.MILLIS_IN_A_MINUTE) {
          return queue;
        }
      }
    }
    for (String queue : queues) {
      if (eligibleForPolling(queue)) {
        Long deactivationTime = queueDeactivationTime.get(queue);
        if (deactivationTime == null) {
          return queue;
        }
        if (now - deactivationTime > pollingInterval) {
          return queue;
        }
      }
    }
    return ALL_QUEUES_ARE_INELIGIBLE;
  }

  private String getQueueToPollOrWait() {
    String queueToPoll = getQueueToPoll();
    if (queueToPoll.equals(ALL_QUEUES_ARE_INELIGIBLE)) {
      if (shouldExit()) {
        return ALL_QUEUES_ARE_INACTIVE;
      }
    }
    log(Level.DEBUG, "Queue to be poll : {}", null, queueToPoll);
    return queueToPoll;
  }

  @Override
  public void start() {
    log(Level.DEBUG, "Running, Ordered Queues: {}", null, queues);
    while (true) {
      try {
        String queue = getQueueToPollOrWait();
        if (queue.equals(ALL_QUEUES_ARE_INACTIVE)) {
          return;
        }
        if (queue.equals(ALL_QUEUES_ARE_INELIGIBLE)) {
          TimeoutUtils.sleepLog(pollingInterval, false);
        } else {
          lastFetchedTime.put(queue, System.currentTimeMillis());
          QueueThreadPool queueThreadPool = queueNameToThread.get(queue);
          QueueDetail queueDetail = queueNameToDetail.get(queue);
          poll(-1, queue, queueDetail, queueThreadPool);
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
  long getSemaphoreWaitTime() {
    return 20L;
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.POLL_FAILED) {
      TimeoutUtils.sleepLog(backoffTime, false);
    } else {
      queueDeactivationTime.put(queue, System.currentTimeMillis());
    }
  }
}
