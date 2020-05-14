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

package com.github.sonus21.rqueue.test.tests;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.Assert.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MessageRetryTest extends SpringTestBase {
  protected void verifyAfterNRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    cleanQueue(jobQueue);
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

  protected void verifyMessageMovedToDelayedQueue() throws TimedOutException {
    cleanQueue(emailQueue);
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

  protected void verifyMessageIsDiscardedAfterRetries() throws TimedOutException {
    cleanQueue(notificationQueue);
    Notification notification = Notification.newInstance();
    failureManager.createFailureDetail(notification.getId(), -1, notificationRetryCount);
    messageSender.enqueueAt(notificationQueue, notification, System.currentTimeMillis() + 1000L);
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

  public void verifySimpleTaskExecution() throws TimedOutException {
    cleanQueue(notificationQueue);
    Notification notification = Notification.newInstance();
    messageSender.enqueue(notificationQueue, notification);
    waitFor(
        () -> {
          Notification notificationInDb =
              consumedMessageService.getMessage(notification.getId(), Notification.class);
          return notification.equals(notificationInDb);
        },
        "notification to be executed");
  }

  public void verifyRetry() throws TimedOutException {
    cleanQueue(emailQueue);
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), emailRetryCount - 1, emailRetryCount - 1);
    messageSender.enqueue(emailQueue, email);
    waitFor(
        () -> failureManager.getFailureCount(email.getId()) == emailRetryCount - 1,
        "email task needs to be retried");
    waitFor(
        () -> {
          Email emailInDb = consumedMessageService.getMessage(email.getId(), Email.class);
          return email.equals(emailInDb);
        },
        "job to be executed");
  }

  public void verifyMessageIsInProcessingQueue() throws TimedOutException {
    cleanQueue(jobQueue);
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), -1, 0);
    messageSender.enqueue(jobQueue, job);
    waitFor(() -> failureManager.getFailureCount(job.getId()) >= 3, "Job to be retried");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(jobQueue);
          return messages.contains(job);
        },
        "message should be present in internal storage");
    // more then one copy should not be present
    assertEquals(1, messageSender.getAllMessages(jobQueue).size());
  }
}
