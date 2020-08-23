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
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.test.RqueueSpringTestRunner;
import java.time.Instant;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@Slf4j
@TestPropertySource(
    properties = {
      "rqueue.retry.per.poll=20",
      "rqueue.scheduler.auto.start=true",
      "spring.redis.port=8008",
      "mysql.db.name=MessageEnqueuerTest",
      "rqueue.metrics.count.failure=false",
      "rqueue.metrics.count.execution=false",
    })
public class MessageEnqueuerTest extends SpringTestBase {

  @Test
  public void testEnqueueWithMessageId() throws TimedOutException {
    Email email = Email.newInstance();
    assertTrue(rqueueMessageEnqueuer.enqueue(emailQueue, email.getId(), email));
    waitFor(() -> getMessageCount(emailQueue) == 0, "email to be consumed");
  }

  @Test
  public void testEnqueueInWithMessageId() throws TimedOutException {
    Email email = Email.newInstance();
    assertTrue(rqueueMessageEnqueuer.enqueueIn(emailQueue, email.getId(), email, 1000));
    waitFor(() -> getMessageCount(emailQueue) == 0, "email to be consumed");
  }

  @Test
  public void testEnqueueAtWithMessageId() throws TimedOutException {
    Notification notification = Notification.newInstance();
    assertTrue(
        rqueueMessageEnqueuer.enqueueAt(
            notificationQueue,
            notification.getId(),
            notification,
            System.currentTimeMillis() + 1000));
    notification = Notification.newInstance();
    assertTrue(
        rqueueMessageEnqueuer.enqueueAt(
            notificationQueue,
            notification.getId(),
            notification,
            Date.from(Instant.now().plusMillis(1000))));
    waitFor(() -> getMessageCount(notificationQueue) == 0, "notification to be consumed");
  }
}
