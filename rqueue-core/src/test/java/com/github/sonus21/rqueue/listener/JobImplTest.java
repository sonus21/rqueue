/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.models.db.Execution;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class JobImplTest extends TestBase {
  @Mock RedisConnectionFactory redisConnectionFactory;
  @Mock private RqueueMessageMetadataService messageMetadataService;
  @Mock private RqueueJobDao rqueueJobDao;
  private RqueueConfig rqueueConfig;
  private final QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  RqueueMessage rqueueMessage =
      RqueueMessageUtils.generateMessage(messageConverter, queueDetail.getName());
  private final MessageMetadata messageMetadata =
      new MessageMetadata(rqueueMessage, MessageStatus.PROCESSING);
  Object userMessage = "Test Object";

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
    rqueueConfig = new RqueueConfig(redisConnectionFactory, null, true, 2);
    FieldUtils.writeField(rqueueConfig, "jobEnabled", true, true);
    FieldUtils.writeField(rqueueConfig, "prefix", "__rq::", true);
    FieldUtils.writeField(rqueueConfig, "jobKeyPrefix", "job::", true);
    FieldUtils.writeField(rqueueConfig, "jobsCollectionNamePrefix", "jobs::", true);
    FieldUtils.writeField(rqueueConfig, "messageDurabilityInTerminalStateInSecond", 900, true);
    FieldUtils.writeField(rqueueConfig, "messageDurabilityInMinute", 10080, true);
    FieldUtils.writeField(rqueueConfig, "jobDurabilityInTerminalStateInSecond", 10080, true);
  }

  @Test
  void construct() {
    new JobImpl(
        rqueueConfig,
        messageMetadataService,
        rqueueJobDao,
        queueDetail,
        messageMetadata,
        rqueueMessage,
        userMessage,
        null,
        null);
    verify(rqueueJobDao, times(1))
        .createJob(any(), eq(Duration.ofMillis(2 * queueDetail.getVisibilityTimeout())));
  }

  @Test
  void getId() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertNotNull(job.getId());
  }

  @Test
  void getRqueueMessage() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertEquals(rqueueMessage, job.getRqueueMessage());
  }

  @Test
  void checkIn() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    job.execute();
    job.checkIn("test..");
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());

    rqueueMessage.setPeriod(100);
    JobImpl job2 =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    job2.execute();
    try {
      job2.checkIn("test..");
      fail("checkin is not supported of periodic task");
    } catch (UnsupportedOperationException ignore) {

    }
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());
  }

  @Test
  void getMessage() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertEquals(userMessage, job.getMessage());
  }

  @Test
  void getMessageMetadata() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertEquals(messageMetadata, job.getMessageMetadata());
  }

  @Test
  void getStatus() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertEquals(JobStatus.CREATED, job.getStatus());
  }

  @Test
  void getException() {
    Exception exception = new Exception("Message");
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            exception,
            null);
    assertEquals(exception, job.getException());
  }

  @Test
  void getExecutionTime() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertEquals(0, job.getExecutionTime());
  }

  @Test
  void getQueueDetail() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    assertEquals(queueDetail, job.getQueueDetail());
  }

  @Test
  void setMessageMetadata() {
    MessageMetadata newMeta = new MessageMetadata(rqueueMessage, MessageStatus.PROCESSING);
    newMeta.setDeleted(true);
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    job.setMessageMetadata(newMeta);
    assertEquals(newMeta, job.getMessageMetadata());
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(1)).save(any(), any());
  }

  @Test
  void updateMessageStatus() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    job.updateMessageStatus(MessageStatus.PROCESSING);
    assertEquals(MessageStatus.PROCESSING, job.getMessageMetadata().getStatus());
    assertEquals(JobStatus.PROCESSING, job.getStatus());
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(messageMetadataService, times(1)).save(any(), any());
    verify(rqueueJobDao, times(1)).save(any(), any());
  }

  @Test
  void execute() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    Execution execution = job.execute();
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(1)).save(any(), any());
    assertNotNull(execution);
    assertNull(execution.getError());
    assertEquals(ExecutionStatus.IN_PROGRESS, execution.getStatus());
  }

  @Test
  void updateExecutionStatus() {
    Exception exception = new Exception("Failing on purpose");
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    Execution execution = job.execute();
    job.updateExecutionStatus(ExecutionStatus.FAILED, exception);
    assertEquals(MessageStatus.PROCESSING, job.getMessageMetadata().getStatus());
    assertEquals(exception, execution.getException());
    assertNotNull(execution.getError());
    assertNotEquals("", execution.getError());
    assertEquals(exception, job.getException());
    assertEquals(ExecutionStatus.FAILED, execution.getStatus());
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());
  }

  @Test
  void updateExecutionTime() {
    JobImpl job =
        new JobImpl(
            rqueueConfig,
            messageMetadataService,
            rqueueJobDao,
            queueDetail,
            messageMetadata,
            rqueueMessage,
            userMessage,
            null,
            null);
    job.execute();
    job.updateExecutionTime(rqueueMessage, MessageStatus.SUCCESSFUL);
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());
    verify(messageMetadataService, times(1)).save(any(), any());
  }
}
