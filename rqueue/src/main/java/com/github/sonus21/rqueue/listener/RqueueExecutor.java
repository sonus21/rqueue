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
import static com.github.sonus21.rqueue.utils.Constants.SECONDS_IN_A_WEEK;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.metrics.RqueueCounter;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.lang.ref.WeakReference;
import java.time.Duration;
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
  private final String messageMetadataId;
  private final Semaphore semaphore;
  private final int retryPerPoll;
  private final TaskExecutionBackOff taskExecutionBackoff;
  private MessageMetadata messageMetadata;
  private Object userMessage;

  RqueueExecutor(
      RqueueMessage rqueueMessage,
      QueueDetail queueDetail,
      Semaphore semaphore,
      WeakReference<RqueueMessageListenerContainer> container,
      RqueueMessageHandler rqueueMessageHandler,
      int retryPerPoll,
      TaskExecutionBackOff taskExecutionBackoff) {
    super(log, queueDetail.getName(), container);
    this.rqueueMessage = rqueueMessage;
    this.queueDetail = queueDetail;
    this.semaphore = semaphore;
    this.rqueueMessageHandler = rqueueMessageHandler;
    this.messageMetadataId = MessageUtils.getMessageMetaId(rqueueMessage.getId());
    this.retryPerPoll = retryPerPoll;
    this.taskExecutionBackoff = taskExecutionBackoff;
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

  private void callMessageProcessor(TaskStatus status, RqueueMessage rqueueMessage) {
    MessageProcessor messageProcessor = null;
    switch (status) {
      case DELETED:
        messageProcessor =
            Objects.requireNonNull(container.get()).getManualDeletionMessageProcessor();
        break;
      case MOVED_TO_DLQ:
        messageProcessor =
            Objects.requireNonNull(container.get()).getDeadLetterQueueMessageProcessor();
        break;
      case DISCARDED:
        messageProcessor = Objects.requireNonNull(container.get()).getDiscardMessageProcessor();
        break;
      case SUCCESSFUL:
        messageProcessor =
            Objects.requireNonNull(container.get()).getPostExecutionMessageProcessor();
        break;
      default:
        break;
    }
    if (messageProcessor != null) {
      try {
        log(Level.DEBUG, "Calling {} processor for {}", null, status, rqueueMessage);
        messageProcessor.process(userMessage, rqueueMessage);
      } catch (Exception e) {
        log(Level.ERROR, "Message processor {} call failed", e, status);
      }
    }
  }

  private void updateCounter(boolean fail) {
    RqueueCounter rqueueCounter = Objects.requireNonNull(container.get()).getRqueueCounter();
    if (rqueueCounter == null) {
      return;
    }
    if (fail) {
      rqueueCounter.updateFailureCount(queueDetail.getName());
    } else {
      rqueueCounter.updateExecutionCount(queueDetail.getName());
    }
  }

  private void publishEvent(TaskStatus status, long jobExecutionStartTime) {
    if (Objects.requireNonNull(container.get()).getRqueueWebConfig().isCollectListenerStats()) {
      addOrDeleteMetadata(jobExecutionStartTime, false);
      RqueueExecutionEvent event =
          new RqueueExecutionEvent(queueDetail, rqueueMessage, status, messageMetadata);
      Objects.requireNonNull(container.get()).getApplicationEventPublisher().publishEvent(event);
    }
  }

  private void addOrDeleteMetadata(long jobExecutionStartTime, boolean saveOrDelete) {
    if (messageMetadata == null) {
      messageMetadata = rqueueMessageMetadataService.get(messageMetadataId);
    }
    if (messageMetadata == null) {
      messageMetadata = new MessageMetadata(messageMetadataId, rqueueMessage.getId());
      // do not call db delete method
      if (!saveOrDelete) {
        messageMetadata.addExecutionTime(jobExecutionStartTime);
        return;
      }
    }
    messageMetadata.addExecutionTime(jobExecutionStartTime);
    if (saveOrDelete) {
      Objects.requireNonNull(container.get())
          .getRqueueMessageMetadataService()
          .save(messageMetadata, Duration.ofSeconds(SECONDS_IN_A_WEEK));
    } else {
      rqueueMessageMetadataService.delete(messageMetadataId);
    }
  }

  private void deleteMessage(TaskStatus status, int failureCount, long jobExecutionStartTime) {
    getRqueueMessageTemplate()
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
    rqueueMessage.setFailureCount(failureCount);
    callMessageProcessor(status, rqueueMessage);
    publishEvent(status, jobExecutionStartTime);
  }

  private void moveMessageToDlq(int failureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (isWarningEnabled()) {
      log(
          Level.WARN,
          "Message {} Moved to dead letter queue: {}",
          null,
          userMessage,
          queueDetail.getDeadLetterQueueName());
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(failureCount);
    newMessage.updateReEnqueuedAt();
    callMessageProcessor(TaskStatus.MOVED_TO_DLQ, newMessage);
    RedisUtils.executePipeLine(
        getRqueueMessageTemplate().getTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          byte[] newMessageBytes = valueSerializer.serialize(newMessage);
          byte[] oldMessageBytes = valueSerializer.serialize(rqueueMessage);
          byte[] processingQueueNameBytes =
              keySerializer.serialize(queueDetail.getProcessingQueueName());
          byte[] dlqNameBytes = keySerializer.serialize(queueDetail.getDeadLetterQueueName());
          connection.rPush(dlqNameBytes, newMessageBytes);
          connection.zRem(processingQueueNameBytes, oldMessageBytes);
        });
    publishEvent(TaskStatus.MOVED_TO_DLQ, jobExecutionStartTime);
  }

  private void parkMessageForRetry(int failureCount, long jobExecutionStartTime, long delay)
      throws CloneNotSupportedException {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} will be retried in {}Ms", null, userMessage, delay);
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(failureCount);
    newMessage.updateReEnqueuedAt();
    getRqueueMessageTemplate()
        .moveMessage(
            queueDetail.getProcessingQueueName(),
            queueDetail.getDelayedQueueName(),
            rqueueMessage,
            newMessage,
            delay);
    addOrDeleteMetadata(jobExecutionStartTime, true);
  }

  private void discardMessage(int failureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} discarded due to retry limit exhaust", null, userMessage);
    }
    deleteMessage(TaskStatus.DISCARDED, failureCount, jobExecutionStartTime);
  }

  private void handleManualDeletion(int failureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message Deleted {} successfully", null, rqueueMessage);
    }
    deleteMessage(TaskStatus.DELETED, failureCount, jobExecutionStartTime);
  }

  private void handleSuccessFullExecution(int failureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message consumed {} successfully", null, rqueueMessage);
    }
    deleteMessage(TaskStatus.SUCCESSFUL, failureCount, jobExecutionStartTime);
  }

  private void handleRetryExceededMessage(int failureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (queueDetail.isDlqSet()) {
      moveMessageToDlq(failureCount, jobExecutionStartTime);
    } else {
      discardMessage(failureCount, jobExecutionStartTime);
    }
  }

  private void handleFailure(int failureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    int maxRetryCount = getMaxRetryCount();
    if (failureCount < maxRetryCount) {
      long delay = taskExecutionBackoff.nextBackOff(userMessage, rqueueMessage, failureCount);
      if (delay == TaskExecutionBackOff.STOP) {
        handleRetryExceededMessage(failureCount, jobExecutionStartTime);
      } else {
        parkMessageForRetry(failureCount, jobExecutionStartTime, delay);
      }
    } else {
      handleRetryExceededMessage(failureCount, jobExecutionStartTime);
    }
  }

  private void handlePostProcessing(
      TaskStatus status, int failureCount, long jobExecutionStartTime) {
    if (status == TaskStatus.QUEUE_INACTIVE) {
      return;
    }
    try {
      switch (status) {
        case SUCCESSFUL:
          handleSuccessFullExecution(failureCount, jobExecutionStartTime);
          break;
        case DELETED:
          handleManualDeletion(failureCount, jobExecutionStartTime);
          break;
        case IGNORED:
          handleIgnoredMessage(failureCount, jobExecutionStartTime);
          break;
        case FAILED:
          handleFailure(failureCount, jobExecutionStartTime);
          break;
        default:
          throw new UnknownSwitchCase(String.valueOf(status));
      }
    } catch (Exception e) {
      log(Level.ERROR, "Error occurred in post processing", e);
    }
  }

  private void handleIgnoredMessage(int failureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} ignored, Queue: {}", null, rqueueMessage, queueDetail.getName());
    }
    deleteMessage(TaskStatus.IGNORED, failureCount, jobExecutionStartTime);
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
      handlePostProcessing(status == null ? TaskStatus.FAILED : status, failureCount, startTime);
    } finally {
      semaphore.release();
    }
  }
}
