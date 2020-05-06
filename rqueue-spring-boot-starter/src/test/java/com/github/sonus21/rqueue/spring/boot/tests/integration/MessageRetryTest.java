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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@TestPropertySource(properties = {"spring.redis.port=6381", "mysql.db.name=test1"})
@SpringBootTest
public class MessageRetryTest extends SpringTestBase {
  @Test
  public void notificationOnMessageIsTriggered() throws TimedOutException {
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
  public void emailIsRetried() throws TimedOutException {
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), emailRetryCount - 1, emailRetryCount - 1);
    messageSender.put(emailQueue, email);
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

  @Test
  public void jobIsRetriedAndMessageIsInProcessingQueue() throws TimedOutException {
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
