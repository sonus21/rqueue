/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.models.event.RqueueExecutionEvent;
import com.github.sonus21.rqueue.utils.PrefixLogger;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import java.io.Serializable;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;
import org.springframework.context.ApplicationEventPublisher;

@Slf4j
@SuppressWarnings("java:S107")
class PostProcessingHandler extends PrefixLogger {

  private final ApplicationEventPublisher applicationEventPublisher;
  private final RqueueWebConfig rqueueWebConfig;
  private final MessageBroker broker;
  private final TaskExecutionBackOff taskExecutionBackoff;
  private final MessageProcessorHandler messageProcessorHandler;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;

  PostProcessingHandler(
      RqueueWebConfig rqueueWebConfig,
      ApplicationEventPublisher applicationEventPublisher,
      MessageBroker broker,
      TaskExecutionBackOff taskExecutionBackoff,
      MessageProcessorHandler messageProcessorHandler,
      RqueueSystemConfigDao rqueueSystemConfigDao) {
    super(log, null);
    this.applicationEventPublisher = applicationEventPublisher;
    this.rqueueWebConfig = rqueueWebConfig;
    this.broker = broker;
    this.taskExecutionBackoff = taskExecutionBackoff;
    this.messageProcessorHandler = messageProcessorHandler;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
  }

  long backOff(
      RqueueMessage rqueueMessage, Object userMessage, int failureCount, Throwable throwable) {
    return taskExecutionBackoff.nextBackOff(userMessage, rqueueMessage, failureCount, throwable);
  }

  void handle(JobImpl job, ExecutionStatus status, int failureCount, Throwable throwable) {
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
          handleFailure(job, failureCount, throwable);
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
        Level.TRACE,
        "Message {} ignored due to old message, Queue: {}",
        null,
        rqueueMessage,
        job.getQueueDetail().getName());
    broker.ack(job.getQueueDetail(), rqueueMessage);
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
    broker.ack(job.getQueueDetail(), rqueueMessage);
    rqueueMessage.setFailureCount(failureCount);
    messageProcessorHandler.handleMessage(job, status);
    publishEvent(job, job.getRqueueMessage(), status);
  }

  private void moveMessageToDlq(JobImpl job, int failureCount, Throwable throwable) {
    log(
        Level.DEBUG,
        "Message {} Moved to dead letter queue: {}",
        null,
        job.getRqueueMessage(),
        job.getQueueDetail().getDeadLetterQueueName());
    RqueueMessage rqueueMessage = job.getRqueueMessage();
    RqueueMessage newMessage =
        rqueueMessage.toBuilder().failureCount(failureCount).build();
    newMessage.updateReEnqueuedAt();
    QueueDetail queueDetail = job.getQueueDetail();
    Object userMessage = job.getMessage();
    messageProcessorHandler.handleMessage(job, MessageStatus.MOVED_TO_DLQ);
    if (queueDetail.isDeadLetterConsumerEnabled()) {
      QueueConfig queueConfig =
          rqueueSystemConfigDao.getConfigByName(queueDetail.getDeadLetterQueueName(), true);
      if (queueConfig == null) {
        log(
            Level.ERROR,
            "Queue Config not found for queue {}",
            null,
            queueDetail.getDeadLetterQueue());
        broker.moveToDlq(
            queueDetail, queueDetail.getDeadLetterQueueName(), rqueueMessage, newMessage, -1);
      } else {
        newMessage.setQueueName(queueConfig.getName());
        newMessage.setFailureCount(0);
        newMessage.setSourceQueueName(rqueueMessage.getQueueName());
        newMessage.setSourceQueueFailureCount(failureCount);
        long backOff = backOff(newMessage, userMessage, failureCount, throwable);
        backOff = (backOff == TaskExecutionBackOff.STOP)
            ? FixedTaskExecutionBackOff.DEFAULT_INTERVAL
            : backOff;
        broker.moveToDlq(
            queueDetail, queueConfig.getScheduledQueueName(), rqueueMessage, newMessage, backOff);
      }
    } else {
      broker.moveToDlq(
          queueDetail, queueDetail.getDeadLetterQueueName(), rqueueMessage, newMessage, -1);
    }
    publishEvent(job, newMessage, MessageStatus.MOVED_TO_DLQ);
  }

  RqueueMessage parkMessageForRetry(
      RqueueMessage rqueueMessage, int failureCount, long delay, QueueDetail queueDetail) {
    RqueueMessage newMessage =
        rqueueMessage.toBuilder().failureCount(failureCount).build().updateReEnqueuedAt();
    broker.parkForRetry(queueDetail, rqueueMessage, newMessage, delay);
    return newMessage;
  }

  void parkMessageForRetry(JobImpl job, Serializable why, int failureCount, long delay) {
    if (why == null) {
      log(Level.TRACE, "Message {} will be retried in {}Ms", null, job.getRqueueMessage(), delay);
    } else {
      log(
          Level.TRACE,
          "Message {} will be retried in {}Ms, Reason: {}",
          null,
          job.getRqueueMessage(),
          delay,
          why);
    }
    RqueueMessage newMessage =
        parkMessageForRetry(job.getRqueueMessage(), failureCount, delay, job.getQueueDetail());
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

  void handleManualDeletion(JobImpl job, int failureCount) {
    log(Level.DEBUG, "Message Deleted {} successfully", null, job.getRqueueMessage());
    deleteMessage(job, MessageStatus.DELETED, failureCount);
  }

  private void handleSuccessFullExecution(JobImpl job, int failureCount) {
    log(Level.DEBUG, "Message consumed {} successfully", null, job.getRqueueMessage());
    deleteMessage(job, MessageStatus.SUCCESSFUL, failureCount);
  }

  private void handleRetryExceededMessage(JobImpl job, int failureCount, Throwable throwable) {
    if (job.getQueueDetail().isDlqSet()) {
      moveMessageToDlq(job, failureCount, throwable);
    } else {
      discardMessage(job, failureCount);
    }
  }

  private int getMaxRetryCount(RqueueMessage rqueueMessage, QueueDetail queueDetail) {
    return rqueueMessage.getRetryCount() == null
        ? queueDetail.getNumRetry()
        : rqueueMessage.getRetryCount();
  }

  private void handleFailure(JobImpl job, int failureCount, Throwable throwable) {
    if (job.getQueueDetail().isDoNotRetryError(throwable)) {
      handleRetryExceededMessage(job, failureCount, throwable);
    } else {
      int maxRetryCount = getMaxRetryCount(job.getRqueueMessage(), job.getQueueDetail());
      if (failureCount < maxRetryCount) {
        long delay = taskExecutionBackoff.nextBackOff(
            job.getMessage(), job.getRqueueMessage(), failureCount, throwable);
        if (delay == TaskExecutionBackOff.STOP) {
          handleRetryExceededMessage(job, failureCount, throwable);
        } else {
          parkMessageForRetry(job, null, failureCount, delay);
        }
      } else {
        handleRetryExceededMessage(job, failureCount, throwable);
      }
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
