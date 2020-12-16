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

package com.github.sonus21.rqueue.core.impl;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.Execution;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.MessageStatus;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;

@Slf4j
@SuppressWarnings("java:S107")
public class JobImpl implements Job {
  private final RqueueJobDao rqueueJobDao;
  private final RqueueMessageMetadataService messageMetadataService;
  private final RqueueConfig rqueueConfig;
  private final QueueDetail queueDetail;
  private final RqueueJob rqueueJob;
  private final Object userMessage;
  public final Duration expiry;
  private final boolean isPeriodicJob;

  public JobImpl(
      RqueueConfig rqueueConfig,
      RqueueMessageMetadataService messageMetadataService,
      RqueueStringDao rqueueStringDao,
      RqueueJobDao rqueueJobDao,
      QueueDetail queueDetail,
      MessageMetadata messageMetadata,
      RqueueMessage rqueueMessage,
      Object userMessage,
      Throwable exception) {
    this.rqueueJobDao = rqueueJobDao;
    this.messageMetadataService = messageMetadataService;
    this.rqueueConfig = rqueueConfig;
    this.queueDetail = queueDetail;
    this.userMessage = userMessage;
    this.rqueueJob =
        new RqueueJob(rqueueConfig.getJobId(), rqueueMessage, messageMetadata, exception);
    this.expiry = Duration.ofMillis(2 * queueDetail.getVisibilityTimeout());
    this.isPeriodicJob = rqueueMessage.isPeriodicTask();
    if (rqueueConfig.isJobEnabled()) {
      if (!isPeriodicJob) {
        rqueueStringDao.appendToListWithListExpiry(
            rqueueConfig.getJobsKey(rqueueMessage.getId()), rqueueJob.getId(), expiry);
        this.save();
      }
    }
  }

  private void save() {
    if (rqueueConfig.isJobEnabled()) {
      if (!isPeriodicJob) {
        try {
          rqueueJob.setUpdatedAt(System.currentTimeMillis());
          rqueueJobDao.save(rqueueJob, expiry);
        } catch (RedisSystemException e) {
          // No op
        }
      }
    }
  }

  @Override
  public String getId() {
    return rqueueJob.getId();
  }

  @Override
  public RqueueMessage getRqueueMessage() {
    return rqueueJob.getRqueueMessage();
  }

  @Override
  public void checkIn(Object message) {
    if(isPeriodicJob){
      throw new UnsupportedOperationException("CheckIn is not supported for periodic job");
    }
    log.debug("Checkin {} Message: {}", rqueueJob.getId(), message);
    this.rqueueJob.checkIn(message);
    this.save();
  }

  @Override
  public Object getMessage() {
    return userMessage;
  }

  @Override
  public MessageMetadata getMessageMetadata() {
    return rqueueJob.getMessageMetadata();
  }

  @Override
  public JobStatus getStatus() {
    return rqueueJob.getStatus();
  }

  @Override
  public Throwable getException() {
    return rqueueJob.getError();
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

  public void setMessageMetadata(MessageMetadata m) {
    this.rqueueJob.setMessageMetadata(m);
    this.save();
  }

  private void setMessageStatus(MessageStatus messageStatus) {
    rqueueJob.setStatus(messageStatus.getJobStatus());
    rqueueJob.getMessageMetadata().setStatus(messageStatus);
  }

  public void updateMessageStatus(MessageStatus messageStatus) {
    Duration messageMetaExpiry;
    if (messageStatus.isTerminalState()) {
      messageMetaExpiry =
          Duration.ofSeconds(rqueueConfig.getMessageDurabilityInTerminalStateInSecond());
    } else {
      messageMetaExpiry = Duration.ofMinutes(rqueueConfig.getMessageDurabilityInMinute());
    }
    setMessageStatus(messageStatus);
    this.messageMetadataService.save(rqueueJob.getMessageMetadata(), messageMetaExpiry);
    save();
  }

  public Execution execute() {
    Execution execution = rqueueJob.startNewExecution();
    save();
    return execution;
  }

  public void updateExecutionStatus(ExecutionStatus status, Throwable e) {
    rqueueJob.updateExecutionStatus(status, e);
    save();
  }

  public void updateExecutionTime(RqueueMessage rqueueMessage, MessageStatus messageStatus) {
    long executionTime = getExecutionTime();
    rqueueJob.getMessageMetadata().setRqueueMessage(rqueueMessage);
    if (getRqueueMessage().isPeriodicTask()) {
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
