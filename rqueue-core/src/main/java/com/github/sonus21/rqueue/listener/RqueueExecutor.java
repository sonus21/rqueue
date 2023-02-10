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

import static com.github.sonus21.rqueue.listener.RqueueMessageHeaders.buildMessageHeaders;
import static com.github.sonus21.rqueue.utils.Constants.DELTA_BETWEEN_RE_ENQUEUE_TIME;
import static com.github.sonus21.rqueue.utils.Constants.ONE_MILLI;
import static com.github.sonus21.rqueue.utils.Constants.REDIS_KEY_SEPARATOR;

import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.middleware.HandlerMiddleware;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.metrics.RqueueMetricsCounter;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import java.util.Collections;
import java.util.List;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;

class RqueueExecutor extends MessageContainerBase {

  private final PostProcessingHandler postProcessingHandler;
  private final QueueThreadPool queueThreadPool;
  private final RqueueMessage rqueueMessage;
  private final RqueueBeanProvider beanProvider;
  private final QueueDetail queueDetail;
  private final List<Middleware> middlewareList;
  private boolean updatedToProcessing;
  private JobImpl job;
  private ExecutionStatus status;
  private Throwable error;
  private int failureCount;

  RqueueExecutor(
      RqueueBeanProvider rqueueBeanProvider,
      QueueStateMgr queueStateMgr,
      List<Middleware> middlewares,
      PostProcessingHandler postProcessingHandler,
      RqueueMessage rqueueMessage,
      QueueDetail queueDetail,
      QueueThreadPool queueThreadPool) {
    super(LoggerFactory.getLogger(RqueueExecutor.class), queueDetail.getName(), queueStateMgr);
    this.middlewareList = middlewares;
    this.postProcessingHandler = postProcessingHandler;
    this.beanProvider = rqueueBeanProvider;
    this.queueThreadPool = queueThreadPool;
    this.rqueueMessage = rqueueMessage;
    this.queueDetail = queueDetail;
  }

  private Object getUserMessage() {
    Message<String> tmpMessage =
        MessageBuilder.createMessage(
            rqueueMessage.getMessage(),
            buildMessageHeaders(
                queueDetail.getName(),
                rqueueMessage,
                null,
                null,
                rqueueMessage.getMessageHeaders()));
    // here error can occur when message can not be deserialized without target class information
    try {
      return RqueueMessageUtils.convertMessageToObject(
          tmpMessage, beanProvider.getRqueueMessageHandler().getMessageConverter());
    } catch (Exception e) {
      log(Level.DEBUG, "Unable to convert message {}", e, rqueueMessage.getMessage());
    }
    return rqueueMessage.getMessage();
  }

  private void init() {
    MessageMetadata messageMetadata =
        beanProvider.getRqueueMessageMetadataService().getOrCreateMessageMetadata(rqueueMessage);
    this.job =
        new JobImpl(
            beanProvider.getRqueueConfig(),
            beanProvider.getRqueueMessageMetadataService(),
            beanProvider.getRqueueJobDao(),
            beanProvider.getRqueueMessageTemplate(),
            beanProvider.getRqueueLockManager(),
            queueDetail,
            messageMetadata,
            rqueueMessage,
            getUserMessage(),
            postProcessingHandler);
    this.failureCount = job.getRqueueMessage().getFailureCount();
  }

  private int getMaxRetryCount() {
    return job.getRqueueMessage().getRetryCount() == null
        ? job.getQueueDetail().getNumRetry()
        : job.getRqueueMessage().getRetryCount();
  }

  private void updateCounter(boolean fail) {
    RqueueMetricsCounter counter = beanProvider.getRqueueMetricsCounter();
    if (counter == null) {
      return;
    }
    if (fail) {
      counter.updateFailureCount(job.getQueueDetail().getName());
    } else {
      counter.updateExecutionCount(job.getQueueDetail().getName());
    }
  }

  private long maxExecutionTime() {
    return job.getQueueDetail().getVisibilityTimeout() - DELTA_BETWEEN_RE_ENQUEUE_TIME;
  }

  private long getMaxProcessingTime() {
    return System.currentTimeMillis() + maxExecutionTime();
  }

  private boolean isMessageDeleted() {
    MessageMetadata messageMetadata = job.getMessageMetadata();
    boolean deleted = messageMetadata.isDeleted();
    if (!deleted) {
      // fetch latest from DB
      MessageMetadata newMessageMetadata =
          beanProvider
              .getRqueueMessageMetadataService()
              .getOrCreateMessageMetadata(job.getRqueueMessage());
      messageMetadata.merge(newMessageMetadata);
    }
    deleted = messageMetadata.isDeleted();
    if (deleted) {
      if (rqueueMessage.isPeriodic()) {
        log(Level.INFO, "Periodic Message {} having period {} has been deleted", null,
            rqueueMessage.getId(), rqueueMessage.getPeriod());
      } else {
        log(Level.INFO, "Message {} has been deleted", null, rqueueMessage.getId());
      }
    }
    return deleted;
  }

  private boolean shouldIgnore() {
    return !beanProvider.getPreExecutionMessageProcessor().process(job);
  }

  private boolean isOldMessage() {
    return job.getMessageMetadata().getRqueueMessage() != null
        && job.getMessageMetadata().getRqueueMessage().getQueuedTime()
        != job.getRqueueMessage().getQueuedTime();
  }

  private int getRetryCount() {
    int maxRetry = getMaxRetryCount();
    if (beanProvider.getRqueueConfig().getRetryPerPoll() == -1) {
      return maxRetry;
    }
    return Math.min(beanProvider.getRqueueConfig().getRetryPerPoll(), maxRetry);
  }

  private boolean queueInactive() {
    return !isQueueActive(job.getQueueDetail().getName());
  }

  private ExecutionStatus getStatus() {
    if (queueInactive()) {
      return ExecutionStatus.QUEUE_INACTIVE;
    }
    if (shouldIgnore()) {
      return ExecutionStatus.IGNORED;
    }
    if (isMessageDeleted()) {
      return ExecutionStatus.DELETED;
    }
    if (isOldMessage()) {
      return ExecutionStatus.OLD_MESSAGE;
    }
    return null;
  }

  private void updateToProcessing() {
    if (updatedToProcessing) {
      return;
    }
    this.updatedToProcessing = true;
    this.job.updateMessageStatus(MessageStatus.PROCESSING);
  }

  private void logExecutionTimeWarning(
      long maxProcessingTime, long startTime, ExecutionStatus status) {
    if (System.currentTimeMillis() > maxProcessingTime) {
      long maxAllowedTime = maxExecutionTime();
      long executionTime = System.currentTimeMillis() - startTime;
      log(
          Level.WARN,
          "Message listener is taking longer time [Queue: {}, TaskStatus: {}] MaxAllowedTime: {}, ExecutionTime: {}",
          null,
          job.getQueueDetail().getName(),
          status,
          maxAllowedTime,
          executionTime);
    }
  }

  private void begin() {
    job.execute();
    this.error = null;
    this.status = getStatus();
  }

  private void end() {
    if (status == null) {
      job.updateExecutionStatus(ExecutionStatus.FAILED, error);
    } else {
      job.updateExecutionStatus(status, error);
    }
  }

  private void callMiddlewares(int currentIndex, List<Middleware> middlewares, Job job)
      throws Exception {
    if (currentIndex == middlewares.size()) {
      new HandlerMiddleware(beanProvider.getRqueueMessageHandler()).handle(job, null);
    } else {
      middlewares
          .get(currentIndex)
          .handle(
              job,
              () -> {
                callMiddlewares(currentIndex + 1, middlewares, job);
                return null;
              });
    }
  }

  private void processMessage() throws Exception {
    if (middlewareList == null) {
      callMiddlewares(0, Collections.emptyList(), job);
    } else {
      callMiddlewares(0, middlewareList, job);
    }
    status = ExecutionStatus.SUCCESSFUL;
  }

  private void execute() {
    try {
      updateToProcessing();
      updateCounter(false);
      processMessage();
    } catch (MessagingException e) {
      updateCounter(true);
      failureCount += 1;
      error = e;
    } catch (Exception e) {
      updateCounter(true);
      failureCount += 1;
      error = e;
      log(Level.ERROR, "Message execution failed, RqueueMessage: {}", e, job.getRqueueMessage());
    }
  }

  private void handleMessage() {
    long maxProcessingTime = getMaxProcessingTime();
    long startTime = System.currentTimeMillis();
    int retryCount = getRetryCount();
    int attempt = 1;
    do {
      log(Level.DEBUG, "Attempt {} message: {}", null, attempt, job.getMessage());
      begin();
      if (status == null) {
        execute();
      }
      retryCount -= 1;
      attempt += 1;
      end();
    } while (retryCount > 0 && status == null && System.currentTimeMillis() < maxProcessingTime);
    postProcessingHandler.handle(
        job, (status == null ? ExecutionStatus.FAILED : status), failureCount);
    logExecutionTimeWarning(maxProcessingTime, startTime, status);
  }

  private long getTtlForScheduledMessageKey(RqueueMessage message) {
    // Assume a message can be executing for at most 2x of their visibility timeout
    // due to failure in some other job same message should not be enqueued
    long expiryInSeconds = 2 * job.getQueueDetail().getVisibilityTimeout() / ONE_MILLI;
    // A message wil be processed after period, so it must stay in the system till that time
    // how many more seconds are left to process this message
    long remainingTime = (message.getProcessAt() - System.currentTimeMillis()) / ONE_MILLI;
    if (remainingTime > 0) {
      expiryInSeconds += remainingTime;
    }
    return expiryInSeconds;
  }

  private String getScheduledMessageKey(RqueueMessage message) {
    // avoid duplicate message enqueue due to retry by checking the message key
    // avoid cross slot error by using tagged queue name in the key
    // enqueuing duplicate message can lead to duplicate consumption when one job is executing task
    // at the same time this message was enqueued.
    return String.format(
        "%s%s%s%ssch%s%d",
        job.getQueueDetail().getQueueName(),
        REDIS_KEY_SEPARATOR,
        job.getRqueueMessage().getId(),
        REDIS_KEY_SEPARATOR,
        REDIS_KEY_SEPARATOR,
        message.getProcessAt());
  }

  private void schedulePeriodicMessage() {
    if (isMessageDeleted()) {
      return;
    }
    RqueueMessage newMessage =
        job.getRqueueMessage().toBuilder()
            .processAt(job.getRqueueMessage().nextProcessAt())
            .build();
    String messageKey = getScheduledMessageKey(newMessage);
    long expiryInSeconds = getTtlForScheduledMessageKey(newMessage);
    log(
        Level.DEBUG,
        "Schedule periodic message: {} Status: {}",
        null,
        job.getRqueueMessage(),
        beanProvider
            .getRqueueMessageTemplate()
            .scheduleMessage(
                job.getQueueDetail().getScheduledQueueName(),
                messageKey,
                newMessage,
                expiryInSeconds));
  }

  private void handlePeriodicMessage() {
    schedulePeriodicMessage();
    handleMessage();
  }

  private void handle() {
    try {
      if (job.getRqueueMessage().isPeriodic()) {
        handlePeriodicMessage();
      } else {
        handleMessage();
      }
    } finally {
      queueThreadPool.release();
    }
  }

  @Override
  public void start() {
    try {
      init();
    } catch (Exception e) {
      log(Level.WARN, "Executor init failed Msg: {}", e, rqueueMessage);
      release(postProcessingHandler, queueThreadPool, queueDetail, rqueueMessage);
      return;
    }
    // TODO could it leak semaphore here?
    handle();
  }
}
