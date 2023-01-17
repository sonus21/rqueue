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
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.CollectionUtils;

abstract class RqueueMessagePoller extends MessageContainerBase {

  final List<Middleware> middlewares;
  final long pollingInterval;
  final long backoffTime;
  private final PostProcessingHandler postProcessingHandler;
  private final RqueueBeanProvider rqueueBeanProvider;
  private final MessageHeaders messageHeaders;
  List<String> queues;

  RqueueMessagePoller(
      String groupName,
      RqueueBeanProvider rqueueBeanProvider,
      QueueStateMgr queueStateMgr,
      List<Middleware> middlewares,
      long pollingInterval,
      long backoffTime,
      PostProcessingHandler postProcessingHandler,
      MessageHeaders messageHeaders) {
    super(LoggerFactory.getLogger(RqueueMessagePoller.class), groupName, queueStateMgr);
    this.postProcessingHandler = postProcessingHandler;
    this.middlewares = middlewares;
    this.rqueueBeanProvider = rqueueBeanProvider;
    this.pollingInterval = pollingInterval;
    this.backoffTime = backoffTime;
    this.messageHeaders = messageHeaders;
  }

  private List<RqueueMessage> getMessages(QueueDetail queueDetail, int count) {
    return rqueueBeanProvider
        .getRqueueMessageTemplate()
        .pop(
            queueDetail.getQueueName(),
            queueDetail.getProcessingQueueName(),
            queueDetail.getProcessingQueueChannelName(),
            queueDetail.getVisibilityTimeout(),
            count);
  }

  private void execute(
      QueueThreadPool queueThreadPool, QueueDetail queueDetail, RqueueMessage message) {
    message.setMessageHeaders(messageHeaders);
    try {
      queueThreadPool.execute(
          new RqueueExecutor(
              rqueueBeanProvider,
              queueStateMgr,
              middlewares,
              postProcessingHandler,
              message,
              queueDetail,
              queueThreadPool));
    } catch (Exception e) {
      if (e instanceof TaskRejectedException) {
        queueThreadPool.taskRejected(queueDetail, message);
      }
      log(Level.WARN, "Execution failed Msg: {}", e, message);
      release(postProcessingHandler, queueThreadPool, queueDetail, message);
    }
  }

  boolean shouldExit() {
    for (String queueName : queues) {
      if (isQueueActive(queueName)) {
        return false;
      }
    }
    log(Level.INFO, "Shutting down all queues {} are inactive", null, queues);
    return true;
  }

  protected boolean hasAvailableThreads(QueueDetail queueDetail, QueueThreadPool queueThreadPool) {
    return queueThreadPool.availableThreads() > 0;
  }


  protected int getBatchSize(QueueDetail queueDetail, QueueThreadPool queueThreadPool) {
    int batchSize = Math.min(queueDetail.getBatchSize(), queueThreadPool.availableThreads());
    batchSize = Math.max(batchSize, Constants.MIN_BATCH_SIZE);
    log(Level.DEBUG, "Batch size {}", null, batchSize);
    return batchSize;
  }

  private void sendMessagesToExecutor(
      QueueDetail queueDetail,
      QueueThreadPool queueThreadPool,
      List<RqueueMessage> rqueueMessages) {
    for (RqueueMessage rqueueMessage : rqueueMessages) {
      execute(queueThreadPool, queueDetail, rqueueMessage);
    }
  }

  // at this point of time, we've acquired batchSize semaphore that means its guarantee that we'll
  // have batchSize available threads in the thread pool
  private void pollAndExecute(
      int index,
      String queue,
      QueueDetail queueDetail,
      QueueThreadPool queueThreadPool,
      int batchSize) {
    if (isQueueActive(queue)) {
      try {
        List<RqueueMessage> messages = getMessages(queueDetail, batchSize);
        log(Level.TRACE, "Queue: {} Fetched Msgs {}", null, queue, messages);
        int messageCount = CollectionUtils.isEmpty(messages) ? 0 : messages.size();
        // free additional requested threads e.g 10 requested but only 5 messages are there
        queueThreadPool.release(batchSize - messageCount);
        if (messageCount > 0) {
          sendMessagesToExecutor(queueDetail, queueThreadPool, messages);
        } else {
          deactivate(index, queue, DeactivateType.NO_MESSAGE);
        }
      } catch (Exception e) {
        queueThreadPool.release(batchSize);
        log(Level.WARN, "Listener failed for the queue {}", e, queue);
        deactivate(index, queue, DeactivateType.POLL_FAILED);
      }
    } else {
      // release resource
      queueThreadPool.release(batchSize);
    }
  }

  void poll(int index, String queue, QueueDetail queueDetail,
      QueueThreadPool queueThreadPool) {
    log(Level.TRACE, "Polling queue {}", null, queue);
    int batchSize = getBatchSize(queueDetail, queueThreadPool);
    boolean acquired;
    try {
      acquired = queueThreadPool.acquire(batchSize, getSemaphoreWaitTime());
    } catch (Exception e) {
      log(Level.WARN, "Exception {}", e, e.getMessage());
      deactivate(index, queue, DeactivateType.SEMAPHORE_EXCEPTION);
      return;
    }
    if (!acquired) {
      deactivate(index, queue, DeactivateType.SEMAPHORE_UNAVAILABLE);
      return;
    }
    pollAndExecute(index, queue, queueDetail, queueThreadPool, batchSize);
  }

  abstract long getSemaphoreWaitTime();

  abstract void deactivate(int index, String queue, DeactivateType deactivateType);

  enum DeactivateType {
    POLL_FAILED,
    NO_MESSAGE,
    SEMAPHORE_EXCEPTION,
    SEMAPHORE_UNAVAILABLE,
  }
}
