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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.db.RqueueJob;
import com.github.sonus21.rqueue.models.enums.ExecutionStatus;
import com.github.sonus21.rqueue.models.enums.JobStatus;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.LongRunningJob;
import com.github.sonus21.rqueue.test.dto.PeriodicJob;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ContextConfiguration(classes = Application.class)
@Slf4j
@TestPropertySource(
    properties = {
        "spring.data.redis.port=8015",
        "mysql.db.name=JobCheckinTest",
        "long.running.job.queue.active=true",
        "use.system.redis=false",
        "monitor.enabled=false",
        "rqueue.retry.per.poll=4",
        "periodic.job.queue.active=true",
        "checkin.enabled=true",
    })
@SpringBootIntegrationTest
class JobCheckinTest extends SpringTestBase {

  @Test
  void jobCheckin() throws TimedOutException {
    LongRunningJob longRunningJob = LongRunningJob.newInstance(2000);
    enqueue(longRunningJobQueue, longRunningJob);
    TimeoutUtils.waitFor(() -> getMessageCount(longRunningJobQueue) == 0, "message to be consumed");
    ConsumedMessage consumedMessage =
        consumedMessageStore.getConsumedMessage(longRunningJob.getId());
    String jobsKey = rqueueConfig.getJobsKey(consumedMessage.getTag());
    List<String> jobIds = stringRqueueRedisTemplate.lrange(jobsKey, 0, -1);
    List<RqueueJob> rqueueJobs = rqueueJobDao.findJobsByIdIn(jobIds);
    assertFalse(rqueueJobs.isEmpty());
    assertFalse(rqueueJobs.get(0).getCheckins().isEmpty());
    assertTrue(rqueueJobs.get(0).getCheckins().size() > 1);
    assertEquals(1, rqueueJobs.get(0).getExecutions().size());
    assertNotNull(rqueueJobs.get(0).getMessageMetadata());
    assertNotNull(rqueueJobs.get(0).getRqueueMessage());
    assertEquals(JobStatus.SUCCESS, rqueueJobs.get(0).getStatus());
    assertNull(rqueueJobs.get(0).getError());
  }

  @Test
  void jobMultipleExecution() throws TimedOutException {
    LongRunningJob longRunningJob = LongRunningJob.newInstance(2000);
    failureManager.createFailureDetail(longRunningJob.getId(), 3, 3);
    enqueue(longRunningJobQueue, longRunningJob);
    TimeoutUtils.waitFor(() -> getMessageCount(longRunningJobQueue) == 0, "message to be consumed");
    ConsumedMessage consumedMessage =
        consumedMessageStore.getConsumedMessage(longRunningJob.getId());
    String jobsKey = rqueueConfig.getJobsKey(consumedMessage.getTag());
    List<String> jobIds = stringRqueueRedisTemplate.lrange(jobsKey, 0, -1);
    List<RqueueJob> rqueueJobs = rqueueJobDao.findJobsByIdIn(jobIds);
    assertFalse(rqueueJobs.isEmpty());
    assertFalse(rqueueJobs.get(0).getCheckins().isEmpty());
    assertTrue(rqueueJobs.get(0).getCheckins().size() > 1);
    assertEquals(4, rqueueJobs.get(0).getExecutions().size());
    assertEquals(ExecutionStatus.FAILED, rqueueJobs.get(0).getExecutions().get(0).getStatus());
    assertEquals(ExecutionStatus.FAILED, rqueueJobs.get(0).getExecutions().get(1).getStatus());
    assertEquals(ExecutionStatus.FAILED, rqueueJobs.get(0).getExecutions().get(2).getStatus());
    assertEquals(ExecutionStatus.SUCCESSFUL, rqueueJobs.get(0).getExecutions().get(3).getStatus());
    assertNotNull(rqueueJobs.get(0).getExecutions().get(0).getError());
    assertNotNull(rqueueJobs.get(0).getExecutions().get(1).getError());
    assertNotNull(rqueueJobs.get(0).getExecutions().get(2).getError());
    assertNull(rqueueJobs.get(0).getExecutions().get(3).getError());
    assertNotNull(rqueueJobs.get(0).getMessageMetadata());
    assertNotNull(rqueueJobs.get(0).getRqueueMessage());
    assertEquals(JobStatus.SUCCESS, rqueueJobs.get(0).getStatus());
    assertNotNull(rqueueJobs.get(0).getError());
  }

  @Test
  void simplePeriodicMessage() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId =
        rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, Duration.ofSeconds(2));
    TimeoutUtils.waitFor(
        () -> {
          printQueueStats(periodicJobQueue);
          printConsumedMessage(periodicJobQueue);
          return consumedMessageStore.getConsumedMessages(job.getId()).size() > 1;
        },
        30_000,
        "at least two execution");
    String jobsKey = rqueueConfig.getJobsKey(messageId);
    List<String> jobIds = stringRqueueRedisTemplate.lrange(jobsKey, 0, -1);
    List<RqueueJob> rqueueJobs = rqueueJobDao.findJobsByIdIn(jobIds);
    assertTrue(rqueueJobs.isEmpty());
  }
}
