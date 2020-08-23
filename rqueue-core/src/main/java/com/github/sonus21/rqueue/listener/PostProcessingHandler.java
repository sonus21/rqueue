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

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.BaseLogger;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;
import org.springframework.context.ApplicationEventPublisher;

@Slf4j
@SuppressWarnings("java:S107")
class PostProcessingHandler extends BaseLogger {
  private final ApplicationEventPublisher applicationEventPublisher;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueMessageMetadataService rqueueMessageMetadataService;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final TaskExecutionBackOff taskExecutionBackoff;
  private final MessageProcessorHandler messageProcessorHandler;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;
  private final RqueueConfig rqueueConfig;

  PostProcessingHandler(
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      ApplicationEventPublisher applicationEventPublisher,
      RqueueMessageMetadataService rqueueMessageMetadataService,
      RqueueMessageTemplate rqueueMessageTemplate,
      TaskExecutionBackOff taskExecutionBackoff,
      MessageProcessorHandler messageProcessorHandler,
      RqueueSystemConfigDao rqueueSystemConfigDao) {
    super(log, null);
    this.applicationEventPublisher = applicationEventPublisher;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueMessageMetadataService = rqueueMessageMetadataService;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.taskExecutionBackoff = taskExecutionBackoff;
    this.messageProcessorHandler = messageProcessorHandler;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
    this.rqueueConfig = rqueueConfig;
  }

  void handle(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      TaskStatus status,
      int failureCount,
      long jobExecutionStartTime) {
    if (status == TaskStatus.QUEUE_INACTIVE) {
      return;
    }
    try {
      switch (status) {
        case SUCCESSFUL:
          handleSuccessFullExecution(
              queueDetail,
              rqueueMessage,
              userMessage,
              messageMetadata,
              failureCount,
              jobExecutionStartTime);
          break;
        case DELETED:
          handleManualDeletion(
              queueDetail,
              rqueueMessage,
              userMessage,
              messageMetadata,
              failureCount,
              jobExecutionStartTime);
          break;
        case IGNORED:
          handleIgnoredMessage(
              queueDetail,
              rqueueMessage,
              userMessage,
              messageMetadata,
              failureCount,
              jobExecutionStartTime);
          break;
        case FAILED:
          handleFailure(
              queueDetail,
              rqueueMessage,
              userMessage,
              messageMetadata,
              failureCount,
              jobExecutionStartTime);
          break;
        default:
          throw new UnknownSwitchCase(String.valueOf(status));
      }
    } catch (Exception e) {
      log(Level.ERROR, "Error occurred in post processing", e);
    }
  }

  private void publishEvent(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      MessageMetadata messageMetadata,
      TaskStatus status,
      long jobExecutionStartTime) {
    updateMetadata(messageMetadata, rqueueMessage, jobExecutionStartTime, false);
    if (rqueueWebConfig.isCollectListenerStats()) {
      RqueueExecutionEvent event =
          new RqueueExecutionEvent(queueDetail, rqueueMessage, status, messageMetadata);
      applicationEventPublisher.publishEvent(event);
    }
  }

  private void updateMetadata(
      MessageMetadata messageMetadata,
      RqueueMessage rqueueMessage,
      long jobExecutionStartTime,
      boolean saveOrDelete) {
    if (!saveOrDelete) {
      messageMetadata.setStatus(TaskStatus.SUCCESSFUL);
    } else {
      messageMetadata.setStatus(TaskStatus.FAILED);
    }
    messageMetadata.setRqueueMessage(rqueueMessage);
    messageMetadata.addExecutionTime(jobExecutionStartTime);
    if (saveOrDelete) {
      rqueueMessageMetadataService.save(
          messageMetadata, Duration.ofMinutes(rqueueConfig.getMessageDurabilityInMinute()));
    } else {
      rqueueMessageMetadataService.save(messageMetadata, Duration.ofMinutes(30));
    }
  }

  private void deleteMessage(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      TaskStatus status,
      int failureCount,
      long jobExecutionStartTime) {
    rqueueMessageTemplate.removeElementFromZset(
        queueDetail.getProcessingQueueName(), rqueueMessage);
    rqueueMessage.setFailureCount(failureCount);
    messageProcessorHandler.handleMessage(rqueueMessage, userMessage, status);
    publishEvent(queueDetail, rqueueMessage, messageMetadata, status, jobExecutionStartTime);
  }

  private void moveMessageToQueue(
      QueueDetail queueDetail,
      String queueName,
      RqueueMessage oldMessage,
      RqueueMessage newMessage) {
    RedisUtils.executePipeLine(
        rqueueMessageTemplate.getTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          byte[] newMessageBytes = valueSerializer.serialize(newMessage);
          byte[] oldMessageBytes = valueSerializer.serialize(oldMessage);
          byte[] processingQueueNameBytes =
              keySerializer.serialize(queueDetail.getProcessingQueueName());
          byte[] queueNameBytes = keySerializer.serialize(queueName);
          connection.rPush(queueNameBytes, newMessageBytes);
          connection.zRem(processingQueueNameBytes, oldMessageBytes);
        });
  }

  private void moveMessageForReprocessingOrDlq(
      QueueDetail queueDetail,
      RqueueMessage oldMessage,
      RqueueMessage newMessage,
      Object userMessage) {
    messageProcessorHandler.handleMessage(newMessage, userMessage, TaskStatus.MOVED_TO_DLQ);
    if (queueDetail.isDeadLetterConsumerEnabled()) {
      String configKey = rqueueConfig.getQueueConfigKey(queueDetail.getDeadLetterQueueName());
      QueueConfig queueConfig = rqueueSystemConfigDao.getQConfig(configKey, true);
      if (queueConfig == null) {
        log(
            Level.ERROR,
            "Queue Config not found for queue {}",
            null,
            queueDetail.getDeadLetterQueue());
        moveMessageToQueue(
            queueDetail, queueDetail.getDeadLetterQueueName(), oldMessage, newMessage);
      } else {
        moveMessageToQueue(queueDetail, queueConfig.getQueueName(), oldMessage, newMessage);
      }
    } else {
      moveMessageToQueue(queueDetail, queueDetail.getDeadLetterQueueName(), oldMessage, newMessage);
    }
  }

  private void moveMessageToDlq(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime)
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
    moveMessageForReprocessingOrDlq(queueDetail, rqueueMessage, newMessage, userMessage);
    publishEvent(
        queueDetail, newMessage, messageMetadata, TaskStatus.MOVED_TO_DLQ, jobExecutionStartTime);
  }

  private void parkMessageForRetry(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime,
      long delay)
      throws CloneNotSupportedException {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} will be retried in {}Ms", null, userMessage, delay);
    }
    RqueueMessage newMessage = rqueueMessage.clone();
    newMessage.setFailureCount(failureCount);
    newMessage.updateReEnqueuedAt();
    rqueueMessageTemplate.moveMessage(
        queueDetail.getProcessingQueueName(),
        queueDetail.getDelayedQueueName(),
        rqueueMessage,
        newMessage,
        delay);
    updateMetadata(messageMetadata, newMessage, jobExecutionStartTime, true);
  }

  private void discardMessage(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} discarded due to retry limit exhaust", null, userMessage);
    }
    deleteMessage(
        queueDetail,
        rqueueMessage,
        userMessage,
        messageMetadata,
        TaskStatus.DISCARDED,
        failureCount,
        jobExecutionStartTime);
  }

  private void handleManualDeletion(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message Deleted {} successfully", null, rqueueMessage);
    }
    deleteMessage(
        queueDetail,
        rqueueMessage,
        userMessage,
        messageMetadata,
        TaskStatus.DELETED,
        failureCount,
        jobExecutionStartTime);
  }

  private void handleSuccessFullExecution(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message consumed {} successfully", null, rqueueMessage);
    }
    deleteMessage(
        queueDetail,
        rqueueMessage,
        userMessage,
        messageMetadata,
        TaskStatus.SUCCESSFUL,
        failureCount,
        jobExecutionStartTime);
  }

  private void handleRetryExceededMessage(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime)
      throws CloneNotSupportedException {
    if (queueDetail.isDlqSet()) {
      moveMessageToDlq(
          queueDetail,
          rqueueMessage,
          userMessage,
          messageMetadata,
          failureCount,
          jobExecutionStartTime);
    } else {
      discardMessage(
          queueDetail,
          rqueueMessage,
          userMessage,
          messageMetadata,
          failureCount,
          jobExecutionStartTime);
    }
  }

  private int getMaxRetryCount(RqueueMessage rqueueMessage, QueueDetail queueDetail) {
    return rqueueMessage.getRetryCount() == null
        ? queueDetail.getNumRetry()
        : rqueueMessage.getRetryCount();
  }

  private void handleFailure(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime)
      throws CloneNotSupportedException {
    int maxRetryCount = getMaxRetryCount(rqueueMessage, queueDetail);
    if (failureCount < maxRetryCount) {
      long delay = taskExecutionBackoff.nextBackOff(userMessage, rqueueMessage, failureCount);
      if (delay == TaskExecutionBackOff.STOP) {
        handleRetryExceededMessage(
            queueDetail,
            rqueueMessage,
            userMessage,
            messageMetadata,
            failureCount,
            jobExecutionStartTime);
      } else {
        parkMessageForRetry(
            queueDetail,
            rqueueMessage,
            userMessage,
            messageMetadata,
            failureCount,
            jobExecutionStartTime,
            delay);
      }
    } else {
      handleRetryExceededMessage(
          queueDetail,
          rqueueMessage,
          userMessage,
          messageMetadata,
          failureCount,
          jobExecutionStartTime);
    }
  }

  private void handleIgnoredMessage(
      QueueDetail queueDetail,
      RqueueMessage rqueueMessage,
      Object userMessage,
      MessageMetadata messageMetadata,
      int failureCount,
      long jobExecutionStartTime) {
    if (isDebugEnabled()) {
      log(Level.DEBUG, "Message {} ignored, Queue: {}", null, rqueueMessage, queueDetail.getName());
    }
    deleteMessage(
        queueDetail,
        rqueueMessage,
        userMessage,
        messageMetadata,
        TaskStatus.IGNORED,
        failureCount,
        jobExecutionStartTime);
  }
}
