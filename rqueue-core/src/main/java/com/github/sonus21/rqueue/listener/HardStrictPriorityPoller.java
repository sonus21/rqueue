/*
 * Copyright (c) 2026 Sonu Kumar
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

/**
 * Use it only with priority queues. Message processing can be slow. The hard strict priority
 * algorithm is better in HardStrictPriorityPoller than in StrictPriorityPoller More details see in
 * <a href="https://github.com/sonus21/rqueue/issues/276">GitHub project issue</a>
 */
class HardStrictPriorityPoller extends RqueueMessagePoller {

  private final RqueueBeanProvider rqueueBeanProvider;

  private final Map<String, QueueDetail> queueNameToDetail;
  private final Map<String, QueueThreadPool> queueNameToThread;
  private final Map<String, Long> queueDeactivationTime = new HashMap<>();
  private final HardStrictPriorityPollerProperties hardStrictPriorityPollerProperties;

  HardStrictPriorityPoller(
      String groupName,
      final List<QueueDetail> queueDetails,
      final Map<String, QueueThreadPool> queueNameToThread,
      RqueueBeanProvider rqueueBeanProvider,
      QueueStateMgr queueStateMgr,
      List<Middleware> middlewares,
      long pollingInterval,
      long backoffTime,
      PostProcessingHandler postProcessingHandler,
      MessageHeaders messageHeaders,
      HardStrictPriorityPollerProperties hardStrictPriorityPollerProperties) {
    super(
        "HardStrict-" + groupName,
        rqueueBeanProvider,
        queueStateMgr,
        middlewares,
        pollingInterval,
        backoffTime,
        postProcessingHandler,
        messageHeaders);

    this.rqueueBeanProvider = rqueueBeanProvider;
    // Sort queues by priority once during initialization
    List<QueueDetail> queueDetailList = new ArrayList<>(queueDetails);
    queueDetailList.sort((o1, o2) -> o2.getPriority().get(Constants.DEFAULT_PRIORITY_KEY)
        - o1.getPriority().get(Constants.DEFAULT_PRIORITY_KEY));

    this.queues = queueDetailList.stream().map(QueueDetail::getName).collect(Collectors.toList());
    this.queueNameToDetail = queueDetailList.stream()
        .collect(Collectors.toMap(QueueDetail::getName, Function.identity()));
    this.queueNameToThread = queueNameToThread;
    this.hardStrictPriorityPollerProperties = hardStrictPriorityPollerProperties != null
        ? hardStrictPriorityPollerProperties
        : new HardStrictPriorityPollerProperties();
  }

  @Override
  public void start() {
    log(Level.DEBUG, "Running, Ordered Queues: {}", null, queues);
    while (true) {
      if (shouldExit()) {
        return;
      }

      boolean messageFoundInAnyQueue = false;

      try {
        for (String queue : queues) {
          if (eligibleForPolling(queue) && !isDeactivated(queue)) {
            QueueThreadPool queueThreadPool = queueNameToThread.get(queue);
            QueueDetail queueDetail = queueNameToDetail.get(queue);
            poll(-1, queue, queueDetail, queueThreadPool);

            if (hardStrictPriorityPollerProperties.getAfterPollSleepInterval() != null) {
              TimeoutUtils.sleepLog(
                  hardStrictPriorityPollerProperties.getAfterPollSleepInterval(), false);
            }

            if (existMessagesInCurrentQueueOrHigherPriorityQueue(queue, queues)) {
              // break current cycle and start new cycle
              // it allow to process queue with the higher priority
              messageFoundInAnyQueue = true;
              break;
            }
          }
        }

        // If no messages were found across all queues, sleep for the polling interval
        if (!messageFoundInAnyQueue) {
          TimeoutUtils.sleepLog(pollingInterval, false);
        }

      } catch (Throwable e) {
        log(Level.ERROR, "Exception in the poller {}", e, e.getMessage());
        if (shouldExit()) {
          return;
        }
        TimeoutUtils.sleepLog(backoffTime, false);
      }
    }
  }

  boolean existMessagesInCurrentQueueOrHigherPriorityQueue(
      String currentQueue, List<String> queues) {
    for (String queue : queues) {
      if (eligibleForPolling(queue) && !isDeactivated(queue)) {
        QueueDetail queueDetail = queueNameToDetail.get(queue);
        if (existAvailableMessagesForPoll(queueDetail)) {
          // the current or higher priority queue contains messages that need to be processed.
          return true;
        }
      }
      // we check all queues from the highest priority to current queue
      if (queue.equals(currentQueue)) {
        return false;
      }
    }
    // unexpected behavior, need more details if it occurs
    log(Level.WARN, "current queue '{}' not found in queues list '{}'", null, currentQueue, queues);
    return false;
  }

  protected boolean existAvailableMessagesForPoll(QueueDetail queueDetail) {
    boolean readyMessagesExists = rqueueBeanProvider
        .getRqueueMessageTemplate()
        .findFirstElementFromList(queueDetail.getQueueName())
        .isPresent();
    if (readyMessagesExists) {
      log(
          Level.TRACE,
          "readyMessages exists for queue '{}', existAvailableMessagesForPoll = true.",
          null,
          queueDetail.getName());
      return true;
    }

    // Only check delayed messages with score <= current time
    long currentTime = System.currentTimeMillis();
    boolean delayedMessagesExists = rqueueBeanProvider
        .getRqueueMessageTemplate()
        .findFirstElementFromZsetWithScore(queueDetail.getScheduledQueueName())
        .filter(element -> element.getScore() <= currentTime)
        .isPresent();

    if (delayedMessagesExists) {
      log(
          Level.TRACE,
          "delayedMessages exists for scheduled queue '{}' and currentTime '{}',"
              + " existAvailableMessagesForPoll = true.",
          null,
          queueDetail.getScheduledQueueName(),
          currentTime);
      return true;
    }

    return false;
  }

  private boolean isDeactivated(String queue) {
    Long deactivationTime = queueDeactivationTime.get(queue);
    if (deactivationTime == null) {
      return false;
    }
    if (System.currentTimeMillis() - deactivationTime > pollingInterval) {
      queueDeactivationTime.remove(queue);
      return false;
    }
    return true;
  }

  @Override
  long getSemaphoreWaitTime() {
    return hardStrictPriorityPollerProperties.getSemaphoreWaitTime() != null
        ? hardStrictPriorityPollerProperties.getSemaphoreWaitTime()
        : 20L;
  }

  @Override
  void deactivate(int index, String queue, DeactivateType deactivateType) {
    if (deactivateType == DeactivateType.POLL_FAILED) {
      // Pause in case of connection errors or polling failures
      TimeoutUtils.sleepLog(backoffTime, false);
    } else {
      // Mark deactivation time if the queue is empty
      queueDeactivationTime.put(queue, System.currentTimeMillis());
    }
  }
}
