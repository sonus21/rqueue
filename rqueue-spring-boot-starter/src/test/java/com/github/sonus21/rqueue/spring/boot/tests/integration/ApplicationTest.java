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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.Assert.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.tests.SpringTestBase;
import com.github.sonus21.test.RqueueSpringTestRunner;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(properties = {"rqueue.retry.per.poll=1000", "spring.redis.port=8001"})
public class ApplicationTest extends SpringTestBase {

  @Test
  public void afterNRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 3, 10);
    messageSender.put(jobQueue, job);
    waitFor(
        () -> {
          Job jobInDb = consumedMessageService.getMessage(job.getId(), Job.class);
          return job.equals(jobInDb);
        },
        "job to be executed");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(jobQueue);
          return !messages.contains(job);
        },
        "message should be deleted from internal storage");
  }

  @Test
  public void messageMovedToDelayedQueue() throws TimedOutException {
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), -1, 0);
    log.debug("queue: {} msg: {}", emailQueue, email);
    messageSender.put(emailQueue, email, 1000L);
    waitFor(
        () -> emailRetryCount == failureManager.getFailureCount(email.getId()),
        30000000,
        "all retry to be exhausted");
    waitFor(
        () -> stringRqueueRedisTemplate.getListSize(emailDeadLetterQueue) > 0,
        "message should be moved to delayed queue");
    assertEquals(emailRetryCount, failureManager.getFailureCount(email.getId()));
    failureManager.delete(email.getId());
  }

  @Test
  public void messageIsDiscardedAfterRetries() throws TimedOutException {
    Notification notification = Notification.newInstance();
    failureManager.createFailureDetail(notification.getId(), -1, notificationRetryCount);
    messageSender.enqueueIn(notificationQueue, notification, 1000L);
    waitFor(
        () -> notificationRetryCount == failureManager.getFailureCount(notification.getId()),
        "all retry to be exhausted");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(notificationQueue);
          return !messages.contains(notification);
        },
        "message to be discarded");
    assertEquals(notificationRetryCount, failureManager.getFailureCount(notification.getId()));
  }
}
