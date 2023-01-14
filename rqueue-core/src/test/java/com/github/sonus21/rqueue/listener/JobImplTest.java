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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.DefaultRqueueMessageConverter;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
@MockitoSettings(strictness = Strictness.LENIENT)
class JobImplTest extends TestBase {

  private final QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
  private final MessageConverter messageConverter = new DefaultRqueueMessageConverter();
  private final RqueueMessage rqueueMessage =
      RqueueMessageUtils.generateMessage(messageConverter, queueDetail.getName());
  private final MessageMetadata messageMetadata =
      new MessageMetadata(rqueueMessage, MessageStatus.PROCESSING);
  private final Object userMessage = "Test Object";
  @Mock
  private RedisConnectionFactory redisConnectionFactory;
  @Mock
  private RqueueMessageMetadataService messageMetadataService;
  @Mock
  private RqueueJobDao rqueueJobDao;
  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Mock
  private RqueueLockManager rqueueLockManager;
  private RqueueConfig rqueueConfig;

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

  private JobImpl instance() {
    return new JobImpl(
        rqueueConfig,
        messageMetadataService,
        rqueueJobDao,
        rqueueMessageTemplate,
        rqueueLockManager,
        queueDetail,
        messageMetadata,
        rqueueMessage,
        userMessage,
        null);
  }

  @Test
  void construct() {
    instance();
    verify(rqueueJobDao, times(1))
        .createJob(any(), eq(Duration.ofMillis(2 * queueDetail.getVisibilityTimeout())));
  }

  @Test
  void getId() {
    JobImpl job = instance();
    assertNotNull(job.getId());
  }

  @Test
  void getRqueueMessage() {
    JobImpl job = instance();
    assertEquals(rqueueMessage, job.getRqueueMessage());
  }

  @Test
  void checkIn() {
    JobImpl job = instance();
    job.execute();
    job.checkIn("test..");
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());

    rqueueMessage.setPeriod(100);
    JobImpl job2 = instance();
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
    JobImpl job = instance();
    assertEquals(userMessage, job.getMessage());
  }

  @Test
  void getMessageMetadata() {
    JobImpl job = instance();
    assertEquals(messageMetadata, job.getMessageMetadata());
  }

  @Test
  void getStatus() {
    JobImpl job = instance();
    assertEquals(JobStatus.CREATED, job.getStatus());
  }

  @Test
  void getExecutionTime() {
    JobImpl job = instance();
    assertEquals(0, job.getExecutionTime());
  }

  @Test
  void getQueueDetail() {
    JobImpl job = instance();
    assertEquals(queueDetail, job.getQueueDetail());
  }

  @Test
  void setMessageMetadata() {
    MessageMetadata newMeta = new MessageMetadata(rqueueMessage, MessageStatus.PROCESSING);
    newMeta.setDeleted(true);
    JobImpl job = instance();
    job.setMessageMetadata(newMeta);
    assertEquals(newMeta, job.getMessageMetadata());
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(1)).save(any(), any());
  }

  @Test
  void updateMessageStatus() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(messageMetadata).when(messageMetadataService).get(messageMetadata.getId());
    JobImpl job = instance();
    job.updateMessageStatus(MessageStatus.PROCESSING);
    assertEquals(MessageStatus.PROCESSING, job.getMessageMetadata().getStatus());
    assertEquals(JobStatus.PROCESSING, job.getStatus());
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(messageMetadataService, times(1)).save(any(), any());
    verify(rqueueJobDao, times(1)).save(any(), any());
  }

  @Test
  void execute() {
    JobImpl job = instance();
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
    JobImpl job = instance();
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
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(messageMetadata).when(messageMetadataService).get(messageMetadata.getId());
    JobImpl job = instance();
    job.execute();
    job.updateExecutionTime(rqueueMessage, MessageStatus.SUCCESSFUL);
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());
    verify(messageMetadataService, times(1))
        .saveMessageMetadataForQueue(anyString(), any(MessageMetadata.class), anyLong());
  }

  @Test
  void getVisibilityTimeout() {
    JobImpl job = instance();
    job.execute();
    doReturn(-10L)
        .when(rqueueMessageTemplate)
        .getScore(queueDetail.getProcessingQueueName(), rqueueMessage);
    assertEquals(Duration.ZERO, job.getVisibilityTimeout());

    doReturn(System.currentTimeMillis() + 10_000L)
        .when(rqueueMessageTemplate)
        .getScore(queueDetail.getProcessingQueueName(), rqueueMessage);
    Duration timeout = job.getVisibilityTimeout();
    assertTrue(timeout.toMillis() <= 10_000 && timeout.toMillis() >= 9_000);

    doReturn(0L)
        .when(rqueueMessageTemplate)
        .getScore(queueDetail.getProcessingQueueName(), rqueueMessage);
    assertEquals(Duration.ZERO, job.getVisibilityTimeout());
  }

  @Test
  void updateVisibilityTimeout() {
    JobImpl job = instance();
    job.execute();
    doReturn(true)
        .when(rqueueMessageTemplate)
        .addScore(queueDetail.getProcessingQueueName(), rqueueMessage, 5_000L);
    assertTrue(job.updateVisibilityTimeout(Duration.ofSeconds(5)));
    doReturn(false)
        .when(rqueueMessageTemplate)
        .addScore(queueDetail.getProcessingQueueName(), rqueueMessage, 5_000L);
    assertFalse(job.updateVisibilityTimeout(Duration.ofSeconds(5)));
  }

  @Test
  void testMessagesAreStoredInMetadataStore() {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(messageMetadata).when(messageMetadataService).get(messageMetadata.getId());
    JobImpl job = instance();
    job.execute();
    job.checkIn("test..");
    job.updateMessageStatus(MessageStatus.SUCCESSFUL);
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(3)).save(any(), any());
    verify(messageMetadataService, times(1))
        .saveMessageMetadataForQueue(
            eq(queueDetail.getCompletedQueueName()),
            any(MessageMetadata.class),
            eq(rqueueConfig.messageDurabilityInTerminalStateInMillisecond()));
  }

  @Test
  void testMessageMetadataIsDeleted() throws IllegalAccessException {
    long currentValue = rqueueConfig.getMessageDurabilityInTerminalStateInSecond();
    FieldUtils.writeField(rqueueConfig, "messageDurabilityInTerminalStateInSecond", 0, true);
    JobImpl job = instance();
    job.execute();
    job.checkIn("test..");
    job.updateMessageStatus(MessageStatus.SUCCESSFUL);
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(3)).save(any(), any());
    verify(messageMetadataService, times(1)).delete(messageMetadata.getId());
    FieldUtils.writeField(
        rqueueConfig, "messageDurabilityInTerminalStateInSecond", currentValue, true);
  }

  @Test
  void testMessageWasDeletedWhileRunning() throws IllegalAccessException {
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    MessageMetadata metadata = messageMetadata.toBuilder().deleted(true)
        .status(MessageStatus.DELETED).build();
    doReturn(metadata).when(messageMetadataService).get(messageMetadata.getId());
    JobImpl job = instance();
    job.execute();
    job.updateMessageStatus(MessageStatus.FAILED);
    verify(rqueueJobDao, times(1)).createJob(any(), any());
    verify(rqueueJobDao, times(2)).save(any(), any());
    doAnswer(invocation -> {
      MessageMetadata messageMetadata = invocation.getArgument(0);
      assertTrue(messageMetadata.isDeleted());
      assertEquals(MessageStatus.DELETED, messageMetadata.getStatus());
      return null;
    }).when(messageMetadataService).save(
        any(MessageMetadata.class),
        eq(Duration.ofMinutes(rqueueConfig.getMessageDurabilityInMinute())));

  }
}
