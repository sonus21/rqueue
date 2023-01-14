/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.context.Context;
import com.github.sonus21.rqueue.core.context.DefaultContext;
import com.github.sonus21.rqueue.core.middleware.TimeProviderMiddleware;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.models.db.Execution;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.util.CollectionUtils;

@Slf4j
@SuppressWarnings("java:S107")
public class JobImpl implements Job {

  public final Duration expiry;
  private final RqueueJobDao rqueueJobDao;
  private final RqueueMessageMetadataService messageMetadataService;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueLockManager rqueueLockManager;
  private final RqueueConfig rqueueConfig;
  private final QueueDetail queueDetail;
  private final RqueueJob rqueueJob;
  private final PostProcessingHandler postProcessingHandler;
  private final boolean isPeriodicJob;
  private Object userMessage;
  private Context context = DefaultContext.EMPTY;
  private Boolean released;
  private Boolean deleted;

  public JobImpl(
      RqueueConfig rqueueConfig,
      RqueueMessageMetadataService messageMetadataService,
      RqueueJobDao rqueueJobDao,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueLockManager rqueueLockManager,
      QueueDetail queueDetail,
      MessageMetadata messageMetadata,
      RqueueMessage rqueueMessage,
      Object userMessage,
      PostProcessingHandler postProcessingHandler) {
    this.rqueueJobDao = rqueueJobDao;
    this.messageMetadataService = messageMetadataService;
    this.rqueueConfig = rqueueConfig;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.queueDetail = queueDetail;
    this.userMessage = userMessage;
    this.postProcessingHandler = postProcessingHandler;
    this.rqueueJob = new RqueueJob(rqueueConfig.getJobId(), rqueueMessage, messageMetadata, null);
    this.expiry = Duration.ofMillis(2 * queueDetail.getVisibilityTimeout());
    this.isPeriodicJob = rqueueMessage.isPeriodic();
    this.rqueueLockManager = rqueueLockManager;
    if (rqueueConfig.isJobEnabled()) {
      if (!isPeriodicJob) {
        rqueueJobDao.createJob(rqueueJob, expiry);
      }
    }
  }

  private void save() {
    if (rqueueConfig.isJobEnabled() && !isPeriodicJob) {
      // tracking intermediate job status
      Duration ttl = expiry;
      if (getMessageMetadata().getStatus().isTerminalState()) {
        ttl = rqueueConfig.getJobDurabilityInTerminalState();
      }
      try {
        if (ttl.isNegative() || ttl.isZero()) {
          rqueueJobDao.delete(rqueueJob.getId());
        } else {
          rqueueJob.setUpdatedAt(System.currentTimeMillis());
          rqueueJobDao.save(rqueueJob, ttl);
        }
      } catch (RedisSystemException e) {
        // No op
      }
    }
  }

  @Override
  public String getId() {
    return rqueueJob.getId();
  }

  @Override
  public String getMessageId() {
    return rqueueJob.getMessageId();
  }

  @Override
  public RqueueMessage getRqueueMessage() {
    return rqueueJob.getRqueueMessage();
  }

  @Override
  public String getRawMessage() {
    return getRqueueMessage().getMessage();
  }

  @Override
  public void checkIn(Serializable message) {
    if (isPeriodicJob) {
      throw new UnsupportedOperationException("CheckIn is not supported for periodic job");
    }
    log.debug("Checkin {} Message: {}", rqueueJob.getId(), message);
    this.rqueueJob.checkIn(message);
    this.save();
  }

  @Override
  public Duration getVisibilityTimeout() {
    Long score =
        rqueueMessageTemplate.getScore(
            queueDetail.getProcessingQueueName(), rqueueJob.getRqueueMessage());
    if (score == null || score <= 0) {
      return Duration.ZERO;
    }
    long remainingTime = score - System.currentTimeMillis();
    return Duration.ofMillis(remainingTime);
  }

  @Override
  public boolean updateVisibilityTimeout(Duration deltaDuration) {
    return rqueueMessageTemplate.addScore(
        queueDetail.getProcessingQueueName(),
        rqueueJob.getRqueueMessage(),
        deltaDuration.toMillis());
  }

  @Override
  public Object getMessage() {
    return userMessage;
  }

  @Override
  public void setMessage(Object message) {
    this.userMessage = message;
  }

  @Override
  public MessageMetadata getMessageMetadata() {
    return rqueueJob.getMessageMetadata();
  }

  void setMessageMetadata(MessageMetadata m) {
    this.rqueueJob.setMessageMetadata(m);
    this.save();
  }

  @Override
  public JobStatus getStatus() {
    return rqueueJob.getStatus();
  }

  @Override
  public Throwable getException() {
    return rqueueJob.getException();
  }

  @Override
  public long getExecutionTime() {
    long executionTime = 0;
    for (Execution execution : rqueueJob.getExecutions()) {
      executionTime += (execution.getEndTime() - execution.getStartTime());
    }
    return executionTime;
  }

  @Override
  public QueueDetail getQueueDetail() {
    return queueDetail;
  }

  @Override
  public Execution getLatestExecution() {
    List<Execution> executions = rqueueJob.getExecutions();
    if (CollectionUtils.isEmpty(executions)) {
      return null;
    }
    return executions.get(executions.size() - 1);
  }

  @Override
  public Context getContext() {
    return context;
  }

  @Override
  public void setContext(Context context) {
    if (context == null) {
      throw new IllegalArgumentException("context can not be null");
    }
    this.context = context;
  }

  @Override
  public void release(JobStatus jobStatus, Serializable why, Duration duration) {
    this.released = true;
    postProcessingHandler.parkMessageForRetry(this, why, getFailureCount(), duration.toMillis());
  }

  @Override
  public void release(JobStatus jobStatus, Serializable why) {
    this.release(jobStatus, why, TimeProviderMiddleware.ONE_SECOND);
  }

  @Override
  public void delete(JobStatus status, Serializable why) {
    this.deleted = true;
    this.postProcessingHandler.handleManualDeletion(this, getFailureCount());
  }

  @Override
  public boolean isDeleted() {
    if (deleted == null) {
      return getMessageMetadata().getStatus().isTerminalState();
    }
    return deleted;
  }

  @Override
  public boolean isReleased() {
    if (released == null) {
      return MessageStatus.FAILED.equals(getMessageMetadata().getStatus());
    }
    return released;
  }

  @Override
  public boolean hasMovedToDeadLetterQueue() {
    return MessageStatus.MOVED_TO_DLQ.equals(getMessageMetadata().getStatus());
  }

  @Override
  public boolean isDiscarded() {
    return MessageStatus.DISCARDED.equals(getMessageMetadata().getStatus());
  }

  private void setMessageStatus(MessageStatus messageStatus) {
    rqueueJob.setStatus(messageStatus.getJobStatus());
    rqueueJob.getMessageMetadata().setStatus(messageStatus);
  }

  @Override
  public int getFailureCount() {
    return getFailureCountInternal();
  }

  private int getFailureCountInternal() {
    if (isDeleted() || isReleased()) {
      return getRqueueMessage().getFailureCount() + rqueueJob.getExecutions().size();
    }
    return getRqueueMessage().getFailureCount();
  }

  private MessageMetadata getLatestMessageMetadata() {
    return messageMetadataService.get(getMessageMetadata().getId());
  }

  private void saveMessageMetadata(Callable<Void> callable) {
    // message is already deleted so we can override no issue
    if (getMessageMetadata().isDeleted()) {
      try {
        callable.call();
      } catch (Exception e) {
        log.error("Saving message metadata failed", e);
      }
      return;
    }

    // check if message has been deleted from some other flow
    long endTime = System.currentTimeMillis() + 10 * Constants.ONE_MILLI;
    long sleepDuration = 100;
    Duration lockDuration = Duration.ofSeconds(1);
    String lockKey = getMessageId();
    String lockValue = UUID.randomUUID().toString();
    while (System.currentTimeMillis() < endTime) {
      if (!rqueueLockManager.acquireLock(lockKey, lockValue, lockDuration)) {
        TimeoutUtils.sleep(sleepDuration);
      } else {
        MessageMetadata localMessageMetadata = getMessageMetadata();
        localMessageMetadata.merge(getLatestMessageMetadata());
        try {
          callable.call();
          // success in update so return
          return;
        } catch (Exception e) {
          log.error("Saving message metadata failed", e);
        } finally {
          rqueueLockManager.releaseLock(lockKey, lockValue);
        }
      }
    }
    // all tries have failed nothing can be done now
    try {
      callable.call();
    } catch (Exception e) {
      log.error("Saving message metadata failed", e);
    }
  }

  void updateMessageStatus(MessageStatus messageStatus) {
    setMessageStatus(messageStatus);
    // We need to address these problems with message metadata
    // 1. Message was deleted while executing, this means local copy is stale
    // 2. Parallel update is being made [dashboard operation, periodic job (two periodic jobs can
    // run in parallel due to failure)]
    if (!messageStatus.isTerminalState() || getRqueueMessage().isPeriodic()) {
      Duration duration = rqueueConfig.getMessageDurability(getRqueueMessage().getPeriod());
      saveMessageMetadata(
          () -> {
            messageMetadataService.save(getMessageMetadata(), duration);
            return null;
          });
    } else {
      long ttl = rqueueConfig.getMessageDurabilityInMinute();
      if (ttl <= 0 || !rqueueConfig.messageInTerminalStateShouldBeStored()) {
        this.messageMetadataService.delete(rqueueJob.getMessageMetadata().getId());
      } else {
        saveMessageMetadata(
            () -> {
              messageMetadataService.saveMessageMetadataForQueue(
                  queueDetail.getCompletedQueueName(),
                  getMessageMetadata(),
                  rqueueConfig.messageDurabilityInTerminalStateInMillisecond());
              return null;
            });
      }
    }
    save();
  }

  Execution execute() {
    Execution execution = rqueueJob.startNewExecution();
    save();
    return execution;
  }

  void updateExecutionStatus(ExecutionStatus status, Throwable e) {
    rqueueJob.updateExecutionStatus(status, e);
    save();
  }

  void updateExecutionTime(RqueueMessage rqueueMessage, MessageStatus messageStatus) {
    long executionTime = getExecutionTime();
    rqueueJob.getMessageMetadata().setRqueueMessage(rqueueMessage);
    if (getRqueueMessage().isPeriodic()) {
      this.rqueueJob.getMessageMetadata().setTotalExecutionTime(executionTime);
    } else {
      this.rqueueJob
          .getMessageMetadata()
          .setTotalExecutionTime(
              executionTime + rqueueJob.getMessageMetadata().getTotalExecutionTime());
    }
    this.updateMessageStatus(messageStatus);
  }
}
