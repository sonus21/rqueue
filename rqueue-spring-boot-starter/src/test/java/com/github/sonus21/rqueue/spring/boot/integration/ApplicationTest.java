/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.spring.boot.integration;

import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;
import static org.junit.Assert.assertEquals;

import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.integration.app.dto.EmailTask;
import com.github.sonus21.rqueue.spring.boot.integration.app.dto.Job;
import com.github.sonus21.rqueue.spring.boot.integration.app.dto.Notification;
import com.github.sonus21.rqueue.spring.boot.integration.app.service.ConsumedMessageService;
import com.github.sonus21.rqueue.spring.boot.integration.app.service.FailureManager;
import com.github.sonus21.rqueue.exception.TimedOutException;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
public class ApplicationTest {
  @Autowired private ConsumedMessageService consumedMessageService;
  @Autowired private RqueueMessageSender messageSender;
  @Autowired private FailureManager failureManager;

  @Value("${job.queue.name}")
  private String jobQueueName;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Value("${email.dead.later.queue.name}")
  private String emailDelayedQueue;

  @Value("${email.queue.retry.count}")
  private int emailRetryCount;

  @Value("${notification.queue.name}")
  private String notificationQueue;

  @Value("${notification.queue.retry.count}")
  private int notificationRetryCount;

  @Test
  public void testJobIsTriggered() throws TimedOutException {
    Job job = Job.newInstance();
    messageSender.put(jobQueueName, job);
    waitFor(
        () -> {
          Job jobInDb = consumedMessageService.getMessage(job.getId(), Job.class);
          return job.equals(jobInDb);
        },
        "job to be executed");
  }

  @Test
  public void testJobIsRetried() throws TimedOutException {
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 3, 10);
    messageSender.put(jobQueueName, job);
    waitFor(() -> failureManager.getFailureCount(job.getId()) >= 3, "Job to be retried");
    waitFor(
        () -> {
          Job jobInDb = consumedMessageService.getMessage(job.getId(), Job.class);
          return job.equals(jobInDb);
        },
        "job to be executed");
  }

  @Test
  public void testJobIsRetriedAndMessageIsInProcessingQueue() throws TimedOutException {
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), -1, 0);
    messageSender.put(jobQueueName, job);
    waitFor(() -> failureManager.getFailureCount(job.getId()) >= 3, "Job to be retried");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(jobQueueName);
          return messages.contains(job);
        },
        "message should be present in internal storage");
    // more then one copy should not be present
    assertEquals(1, messageSender.getAllMessages(jobQueueName).size());
  }

  @Test
  public void testAfterRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 3, 10);
    messageSender.put(jobQueueName, job);
    waitFor(
        () -> {
          Job jobInDb = consumedMessageService.getMessage(job.getId(), Job.class);
          return job.equals(jobInDb);
        },
        "job to be executed");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(jobQueueName);
          return !messages.contains(job);
        },
        "message should be deleted from internal storage");
  }

  @Test
  public void testMessageMovedToDelayedQueue() throws TimedOutException {
    EmailTask emailTask = EmailTask.newInstance();
    failureManager.createFailureDetail(emailTask.getId(), -1, 0);
    messageSender.put(emailQueue, emailTask, 1000L);
    waitFor(
        () -> emailRetryCount == failureManager.getFailureCount(emailTask.getId()),
        "all retry to be exhausted");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(emailDelayedQueue);
          return messages.contains(emailTask);
        },
        "message should be moved to delayed queue");
    assertEquals(emailRetryCount, failureManager.getFailureCount(emailTask.getId()));
  }

  @Test
  public void testMessageIsDiscardedAfterRetries() throws TimedOutException {
    Notification notification = Notification.newInstance();
    failureManager.createFailureDetail(notification.getId(), -1, 0);
    messageSender.put(notificationQueue, notification, 1000L);
    waitFor(
        () -> emailRetryCount == failureManager.getFailureCount(notification.getId()),
        "all retry to be exhausted");
    waitFor(
        () -> {
          List<Object> messages = messageSender.getAllMessages(notificationQueue);
          return !messages.contains(notification);
        },
        "message to be discarded");
    assertEquals(emailRetryCount, failureManager.getFailureCount(notification.getId()));
  }
}
