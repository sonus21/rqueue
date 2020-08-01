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

import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.metrics.RqueueMetricsCounter;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

@Slf4j
class RqueueExecutor extends MessageContainerBase {
  private final QueueDetail queueDetail;
  private final Message<String> message;
  private final RqueueMessage rqueueMessage;
  private final RqueueMessageHandler rqueueMessageHandler;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;
  private final PostProcessingHandler postProcessingHandler;
  private final String messageMetadataId;
  private final Semaphore semaphore;
  private final int retryPerPoll;
  private MessageMetadata messageMetadata;
  private Object userMessage;

  RqueueExecutor(
      RqueueMessage rqueueMessage,
      QueueDetail queueDetail,
      Semaphore semaphore,
      WeakReference<RqueueMessageListenerContainer> container,
      int retryPerPoll,
      PostProcessingHandler postProcessingHandler) {
    super(log, queueDetail.getName(), container);
    this.rqueueMessage = rqueueMessage;
    this.queueDetail = queueDetail;
    this.semaphore = semaphore;
    this.rqueueMessageHandler = Objects.requireNonNull(container.get()).getRqueueMessageHandler();
    this.messageMetadataId = MessageUtils.getMessageMetaId(rqueueMessage.getId());
    this.retryPerPoll = retryPerPoll;
    this.postProcessingHandler = postProcessingHandler;
    this.message =
        MessageBuilder.createMessage(
            rqueueMessage.getMessage(),
            RqueueMessageHeaders.buildMessageHeaders(queueDetail.getName(), rqueueMessage));
    try {
      this.userMessage =
          MessageUtils.convertMessageToObject(message, rqueueMessageHandler.getMessageConverter());
    } catch (Exception e) {
      log(Level.ERROR, "Unable to convert message {}", e, rqueueMessage.getMessage());
    }
    this.rqueueMessageMetadataService =
        Objects.requireNonNull(container.get()).getRqueueMessageMetadataService();
  }

  private int getMaxRetryCount() {
    return rqueueMessage.getRetryCount() == null
        ? queueDetail.getNumRetry()
        : rqueueMessage.getRetryCount();
  }

  private void updateCounter(boolean fail) {
    RqueueMetricsCounter counter =
        Objects.requireNonNull(container.get()).getRqueueMetricsCounter();
    if (counter == null) {
      return;
    }
    if (fail) {
      counter.updateFailureCount(queueDetail.getName());
    } else {
      counter.updateExecutionCount(queueDetail.getName());
    }
  }

  private long getMaxProcessingTime() {
    return System.currentTimeMillis()
        + queueDetail.getVisibilityTimeout()
        - DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }

  private boolean isMessageDeleted() {
    messageMetadata = rqueueMessageMetadataService.get(messageMetadataId);
    if (messageMetadata == null) {
      return false;
    }
    return messageMetadata.isDeleted();
  }

  private boolean shouldIgnore() {
    return !Objects.requireNonNull(container.get())
        .getPreExecutionMessageProcessor()
        .process(userMessage, rqueueMessage);
  }

  private int getRetryCount() {
    int maxRetry = getMaxRetryCount();
    if (retryPerPoll == -1) {
      return maxRetry;
    }
    return Math.min(retryPerPoll, maxRetry);
  }

  private boolean queueActive() {
    return isQueueActive(queueDetail.getName());
  }

  private TaskStatus getStatus() {
    if (!queueActive()) {
      return TaskStatus.QUEUE_INACTIVE;
    }
    if (shouldIgnore()) {
      return TaskStatus.IGNORED;
    }
    if (isMessageDeleted()) {
      return TaskStatus.DELETED;
    }
    return null;
  }

  @Override
  void start() {
    int failureCount = rqueueMessage.getFailureCount();
    long maxProcessingTime = getMaxProcessingTime();
    long startTime = System.currentTimeMillis();
    int retryCount = getRetryCount();
    TaskStatus status;
    try {
      do {
        status = getStatus();
        if (status != null) {
          break;
        }
        try {
          updateCounter(false);
          rqueueMessageHandler.handleMessage(message);
          status = TaskStatus.SUCCESSFUL;
        } catch (MessagingException e) {
          updateCounter(true);
          failureCount += 1;
        } catch (Exception e) {
          updateCounter(true);
          failureCount += 1;
          log(Level.ERROR, "Message execution failed", e);
        }
        retryCount--;
      } while (retryCount > 0 && status == null && System.currentTimeMillis() < maxProcessingTime);
      postProcessingHandler.handlePostProcessing(
          queueDetail,
          rqueueMessage,
          userMessage,
          messageMetadata,
          status == null ? TaskStatus.FAILED : status,
          failureCount,
          startTime);
    } finally {
      semaphore.release();
    }
  }
}
