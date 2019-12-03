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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static com.github.sonus21.rqueue.utils.TimeUtil.waitFor;
import static org.junit.Assert.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.producer.RqueueMessageSender;
import com.github.sonus21.rqueue.spring.boot.application.app.dto.EmailTask;
import com.github.sonus21.rqueue.spring.boot.application.app.dto.Job;
import com.github.sonus21.rqueue.spring.boot.application.app.dto.Notification;
import com.github.sonus21.rqueue.spring.boot.application.app.service.ConsumedMessageService;
import com.github.sonus21.rqueue.spring.boot.application.app.service.FailureManager;
import com.github.sonus21.rqueue.spring.boot.application.main.Application;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource(properties = {"spring.redis.port=6381", "mysql.db.name=test1"})
@SpringBootTest
public class ApplicationTest2 {
  static {
    System.setProperty("TEST_NAME", "ApplicationTest2");
  }
  @Autowired private ConsumedMessageService consumedMessageService;
  @Autowired private RqueueMessageSender messageSender;
  @Autowired private FailureManager failureManager;

  @Value("${job.queue.name}")
  private String jobQueueName;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Value("${email.queue.retry.count}")
  private int emailRetryCount;

  @Value("${notification.queue.name}")
  private String notificationQueue;

  @Value("${notification.queue.retry.count}")
  private int notificationRetryCount;

  @Test
  public void testNotificationIsTriggered() throws TimedOutException {
    Notification notification = Notification.newInstance();
    messageSender.put(notificationQueue, notification);
    waitFor(
        () -> {
          Notification notificationInDb =
              consumedMessageService.getMessage(notification.getId(), Notification.class);
          return notification.equals(notificationInDb);
        },
        "notification to be executed");
  }

  @Test
  public void testEmailIsRetried() throws TimedOutException {
    EmailTask emailTask = EmailTask.newInstance();
    failureManager.createFailureDetail(emailTask.getId(), emailRetryCount - 1, emailRetryCount - 1);
    messageSender.put(emailQueue, emailTask);
    waitFor(
        () -> failureManager.getFailureCount(emailTask.getId()) == emailRetryCount - 1,
        "email task needs to be retried");
    waitFor(
        () -> {
          EmailTask emailTaskInDb =
              consumedMessageService.getMessage(emailTask.getId(), EmailTask.class);
          return emailTask.equals(emailTaskInDb);
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
}
