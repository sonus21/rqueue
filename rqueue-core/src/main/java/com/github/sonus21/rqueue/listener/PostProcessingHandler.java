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
import com.github.sonus21.rqueue.core.impl.JobImpl;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.db.MessageStatus;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.PrefixLogger;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;
import org.springframework.context.ApplicationEventPublisher;

@Slf4j
@SuppressWarnings("java:S107")
class PostProcessingHandler extends PrefixLogger {
  private final ApplicationEventPublisher applicationEventPublisher;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final TaskExecutionBackOff taskExecutionBackoff;
  private final MessageProcessorHandler messageProcessorHandler;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;
  private final RqueueConfig rqueueConfig;

  PostProcessingHandler(
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      ApplicationEventPublisher applicationEventPublisher,
      RqueueMessageTemplate rqueueMessageTemplate,
      TaskExecutionBackOff taskExecutionBackoff,
      MessageProcessorHandler messageProcessorHandler,
      RqueueSystemConfigDao rqueueSystemConfigDao) {
    super(log, null);
    this.applicationEventPublisher = applicationEventPublisher;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.taskExecutionBackoff = taskExecutionBackoff;
    this.messageProcessorHandler = messageProcessorHandler;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
    this.rqueueConfig = rqueueConfig;
  }

  void handle(JobImpl job, ExecutionStatus status, int failureCount) {
    try {
      switch (status) {
        case QUEUE_INACTIVE:
          return;
        case DELETED:
          handleManualDeletion(job, failureCount);
          break;
        case IGNORED:
          handleIgnoredMessage(job, failureCount);
          break;
        case OLD_MESSAGE:
          handleOldMessage(job, job.getRqueueMessage());
          break;
        case SUCCESSFUL:
          handleSuccessFullExecution(job, failureCount);
          break;
        case FAILED:
          handleFailure(job, failureCount);
          break;
        default:
          throw new UnknownSwitchCase(String.valueOf(status));
      }
    } catch (Exception e) {
      log(
          Level.ERROR,
          "Error occurred in post processing, RqueueMessage: {}, Status: {}",
          e,
          job.getRqueueMessage(),
          status);
    }
  }

  private void handleOldMessage(JobImpl job, RqueueMessage rqueueMessage) {
    log(
        Level.DEBUG,
        "Message {} ignored due to old message, Queue: {}",
        null,
        rqueueMessage,
        job.getQueueDetail().getName());
    rqueueMessageTemplate.removeElementFromZset(
        job.getQueueDetail().getProcessingQueueName(), rqueueMessage);
  }

  private void publishEvent(JobImpl job, RqueueMessage rqueueMessage, MessageStatus messageStatus) {
    updateMetadata(job, rqueueMessage, messageStatus);
    if (rqueueWebConfig.isCollectListenerStats()) {
      RqueueExecutionEvent event = new RqueueExecutionEvent(job);
      applicationEventPublisher.publishEvent(event);
    }
  }

  private void updateMetadata(
      JobImpl job, RqueueMessage rqueueMessage, MessageStatus messageStatus) {
    job.updateExecutionTime(rqueueMessage, messageStatus);
  }

  private void deleteMessage(JobImpl job, MessageStatus status, int failureCount) {
    RqueueMessage rqueueMessage = job.getRqueueMessage();
    rqueueMessageTemplate.removeElementFromZset(
        job.getQueueDetail().getProcessingQueueName(), rqueueMessage);
    rqueueMessage.setFailureCount(failureCount);
    messageProcessorHandler.handleMessage(rqueueMessage, job.getMessage(), status);
    publishEvent(job, job.getRqueueMessage(), status);
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
          assert queueNameBytes != null;
          connection.rPush(queueNameBytes, newMessageBytes);
          assert processingQueueNameBytes != null;
          connection.zRem(processingQueueNameBytes, oldMessageBytes);
        });
  }

  private void moveMessageForReprocessingOrDlq(
      QueueDetail queueDetail,
      RqueueMessage oldMessage,
      RqueueMessage newMessage,
      Object userMessage) {
    messageProcessorHandler.handleMessage(newMessage, userMessage, MessageStatus.MOVED_TO_DLQ);
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

  private void moveMessageToDlq(JobImpl job, int failureCount) {
    log(
        Level.WARN,
        "Message {} Moved to dead letter queue: {}",
        null,
        job.getRqueueMessage(),
        job.getQueueDetail().getDeadLetterQueueName());
    RqueueMessage rqueueMessage = job.getRqueueMessage();
    RqueueMessage newMessage = rqueueMessage.toBuilder().failureCount(failureCount).build();
    newMessage.updateReEnqueuedAt();
    moveMessageForReprocessingOrDlq(
        job.getQueueDetail(), rqueueMessage, newMessage, job.getMessage());
    publishEvent(job, newMessage, MessageStatus.MOVED_TO_DLQ);
  }

  private void parkMessageForRetry(JobImpl job, int failureCount, long delay) {
    log(Level.DEBUG, "Message {} will be retried in {}Ms", null, job.getRqueueMessage(), delay);
    RqueueMessage rqueueMessage = job.getRqueueMessage();
    RqueueMessage newMessage = rqueueMessage.toBuilder().failureCount(failureCount).build();
    newMessage.updateReEnqueuedAt();
    rqueueMessageTemplate.moveMessage(
        job.getQueueDetail().getProcessingQueueName(),
        job.getQueueDetail().getDelayedQueueName(),
        rqueueMessage,
        newMessage,
        delay);
    updateMetadata(job, newMessage, MessageStatus.FAILED);
  }

  private void discardMessage(JobImpl job, int failureCount) {
    log(
        Level.DEBUG,
        "Message {} discarded due to retry limit exhaust",
        null,
        job.getRqueueMessage());
    deleteMessage(job, MessageStatus.DISCARDED, failureCount);
  }

  private void handleManualDeletion(JobImpl job, int failureCount) {
    log(Level.DEBUG, "Message Deleted {} successfully", null, job.getRqueueMessage());
    deleteMessage(job, MessageStatus.DELETED, failureCount);
  }

  private void handleSuccessFullExecution(JobImpl job, int failureCount) {
    log(Level.DEBUG, "Message consumed {} successfully", null, job.getRqueueMessage());
    deleteMessage(job, MessageStatus.SUCCESSFUL, failureCount);
  }

  private void handleRetryExceededMessage(JobImpl job, int failureCount) {
    if (job.getQueueDetail().isDlqSet()) {
      moveMessageToDlq(job, failureCount);
    } else {
      discardMessage(job, failureCount);
    }
  }

  private int getMaxRetryCount(RqueueMessage rqueueMessage, QueueDetail queueDetail) {
    return rqueueMessage.getRetryCount() == null
        ? queueDetail.getNumRetry()
        : rqueueMessage.getRetryCount();
  }

  private void handleFailure(JobImpl job, int failureCount) {
    int maxRetryCount = getMaxRetryCount(job.getRqueueMessage(), job.getQueueDetail());
    if (failureCount < maxRetryCount) {
      long delay =
          taskExecutionBackoff.nextBackOff(job.getMessage(), job.getRqueueMessage(), failureCount);
      if (delay == TaskExecutionBackOff.STOP) {
        handleRetryExceededMessage(job, failureCount);
      } else {
        parkMessageForRetry(job, failureCount, delay);
      }
    } else {
      handleRetryExceededMessage(job, failureCount);
    }
  }

  private void handleIgnoredMessage(JobImpl job, int failureCount) {
    log(
        Level.DEBUG,
        "Message {} ignored, Queue: {}",
        null,
        job.getRqueueMessage(),
        job.getQueueDetail().getName());
    deleteMessage(job, MessageStatus.IGNORED, failureCount);
  }
}
