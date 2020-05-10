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
import static org.springframework.util.Assert.notNull;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
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
import org.springframework.messaging.support.GenericMessage;

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
        new GenericMessage<>(
            rqueueMessage.getMessage(), MessageUtils.getMessageHeader(queueDetail.getName()));
    try {
      this.userMessage =
          MessageUtils.convertMessageToObject(message, rqueueMessageHandler.getMessageConverters());
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
        messageProcessor.process(userMessage);
      } catch (Exception e) {
        log(Level.ERROR, "Message processor {} call failed", e, status);
      }
    }
  }

  @SuppressWarnings("ConstantConditions")
  private void updateCounter(boolean failOrExecution) {
    RqueueCounter rqueueCounter = container.get().getRqueueCounter();
    if (rqueueCounter == null) {
      return;
    }
    if (failOrExecution) {
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

  private void addOrDeleteMetadata(long jobExecutionTime, boolean saveOrDelete) {
    if (messageMetadata == null) {
      messageMetadata = rqueueMessageMetadataService.get(messageMetadataId);
    }
    if (messageMetadata == null) {
      messageMetadata = new MessageMetadata(messageMetadataId, rqueueMessage.getId());
      // do not call db delete method
      if (!saveOrDelete) {
        messageMetadata.addExecutionTime(jobExecutionTime);
        return;
      }
    }
    messageMetadata.addExecutionTime(jobExecutionTime);
    if (saveOrDelete) {
      Objects.requireNonNull(container.get())
          .getRqueueMessageMetadataService()
          .save(messageMetadata, Duration.ofSeconds(SECONDS_IN_A_WEEK));
    } else {
      rqueueMessageMetadataService.delete(messageMetadataId);
    }
  }

  private void deleteMessage(
      TaskStatus status, int currentFailureCount, long jobExecutionStartTime) {
    getRqueueMessageTemplate()
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
    rqueueMessage.setFailureCount(currentFailureCount);
    callMessageProcessor(status, rqueueMessage);
    publishEvent(status, jobExecutionStartTime);
  }

  private void moveMessageToDlq(int currentFailureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (isWarningEnabled()) {
      log(
          Level.WARN,
          "Message {} Moved to dead letter queue: {}, dead letter queue: {}",
          null,
          userMessage,
          queueDetail.getName(),
          queueDetail.getDeadLetterQueueName());
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(currentFailureCount);
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

  private void parkMessageForRetry(int currentFailureCount, long jobExecutionStartTime, long delay)
      throws CloneNotSupportedException {
    if (isDebugEnabled()) {
      log(
          Level.DEBUG,
          "Message {} will be retried in {}Ms, queue: {}, Redis Queue: {}",
          null,
          userMessage,
          delay,
          queueDetail.getName(),
          queueDetail.getQueueName());
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(currentFailureCount);
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

  private void discardMessage(int currentFailureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(
          Level.DEBUG,
          "Message {} discarded due to retry limit exhaust queue: {}",
          null,
          userMessage,
          queueDetail.getName());
    }
    deleteMessage(TaskStatus.DISCARDED, currentFailureCount, jobExecutionStartTime);
  }

  private void handleManualDeletion(int currentFailureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(
          Level.DEBUG,
          "Message Deleted manually {} successfully, Queue: {}",
          null,
          rqueueMessage,
          queueDetail.getName());
    }
    deleteMessage(TaskStatus.DELETED, currentFailureCount, jobExecutionStartTime);
  }

  private void handleSuccessFullExecution(int currentFailureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(
          Level.DEBUG,
          "Message consumed {} successfully, Queue: {}",
          null,
          rqueueMessage,
          queueDetail.getName());
    }
    deleteMessage(TaskStatus.SUCCESSFUL, currentFailureCount, jobExecutionStartTime);
  }

  private void handleLimitExceededMessage(int currentFailureCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (queueDetail.isDlqSet()) {
      moveMessageToDlq(currentFailureCount, jobExecutionStartTime);
    } else {
      discardMessage(currentFailureCount, jobExecutionStartTime);
    }
  }

  private void handleFailure(int currentFailureCount, int maxRetryCount, long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (currentFailureCount < maxRetryCount) {
      long delay =
          taskExecutionBackoff.nextBackOff(userMessage, rqueueMessage, currentFailureCount);
      if (delay == TaskExecutionBackOff.STOP) {
        handleLimitExceededMessage(currentFailureCount, jobExecutionStartTime);
      } else {
        parkMessageForRetry(currentFailureCount, jobExecutionStartTime, delay);
      }
    } else {
      handleLimitExceededMessage(currentFailureCount, jobExecutionStartTime);
    }
  }

  private void handlePostProcessing(
      boolean executed,
      boolean deleted,
      boolean ignored,
      int currentFailureCount,
      int maxRetryCount,
      long jobExecutionStartTime) {
    if (!isQueueActive(queueDetail.getName())) {
      return;
    }
    try {
      if (ignored) {
        handleIgnoredMessage(currentFailureCount, jobExecutionStartTime);
      } else if (deleted) {
        handleManualDeletion(currentFailureCount, jobExecutionStartTime);
      } else {
        if (!executed) {
          handleFailure(currentFailureCount, maxRetryCount, jobExecutionStartTime);
        } else {
          handleSuccessFullExecution(currentFailureCount, jobExecutionStartTime);
        }
      }
    } catch (Exception e) {
      log(Level.ERROR, "Error occurred in post processing", e);
    }
  }

  private void handleIgnoredMessage(int currentFailureCount, long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} ignored, Queue: {}", null, rqueueMessage, queueDetail.getName());
    }
    deleteMessage(TaskStatus.IGNORED, currentFailureCount, jobExecutionStartTime);
  }

  private long getMaxProcessingTime() {
    return System.currentTimeMillis()
        + queueDetail.getVisibilityTimeout()
        - DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }

  private boolean isMessageDeleted(String id) {
    notNull(id, "Message id must be present");
    messageMetadata = rqueueMessageMetadataService.get(messageMetadataId);
    if (messageMetadata == null) {
      return false;
    }
    return messageMetadata.isDeleted();
  }

  private boolean shouldProcess() {
    return Objects.requireNonNull(container.get())
        .getPreExecutionMessageProcessor()
        .process(userMessage);
  }

  private int getRetryCount() {
    int maxRetry = getMaxRetryCount();
    if (retryPerPoll == -1) {
      return maxRetry;
    }
    return Math.min(retryPerPoll, maxRetry);
  }

  @Override
  void start() {
    boolean executed = false;
    int currentFailureCount = rqueueMessage.getFailureCount();
    int maxRetryCount = getMaxRetryCount();
    long maxRetryTime = getMaxProcessingTime();
    long startTime = System.currentTimeMillis();
    boolean deleted = false;
    boolean ignored = false;
    int retryCount = getRetryCount();
    try {
      do {
        if (!isQueueActive(queueDetail.getName())) {
          return;
        }
        if (!shouldProcess()) {
          ignored = true;
        } else if (isMessageDeleted(rqueueMessage.getId())) {
          deleted = true;
        }
        if (ignored || deleted) {
          break;
        }
        try {
          updateCounter(false);
          rqueueMessageHandler.handleMessage(message);
          executed = true;
        } catch (Exception e) {
          log(Level.ERROR, "Message consumer failed", e);
          updateCounter(true);
          currentFailureCount += 1;
        }
        retryCount--;
      } while (currentFailureCount < maxRetryCount
          && retryCount > 0
          && !executed
          && System.currentTimeMillis() < maxRetryTime);
      handlePostProcessing(
          executed, deleted, ignored, currentFailureCount, maxRetryCount, startTime);
    } finally {
      semaphore.release();
    }
  }
}
