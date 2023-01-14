/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.sleep;
import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Notification;
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
        "rqueue.retry.per.poll=20",
        "rqueue.scheduler.auto.start=true",
        "spring.data.redis.port=8009",
        "mysql.db.name=MessageDeduplicationTest",
        "rqueue.metrics.count.failure=false",
        "rqueue.metrics.count.execution=false",
    })
@SpringBootIntegrationTest
class MessageDeduplicationTest extends SpringTestBase {

  @Test
  void enqueueUnique() throws TimedOutException {
    Email email = Email.newInstance();
    rqueueMessageEnqueuer.enqueueUnique(emailQueue, email.getId(), email);
    waitFor(() -> getMessageCount(emailQueue) == 0, "email to be sent");
  }

  @Test
  void enqueueUniqueIn() throws TimedOutException {
    Notification notification = Notification.newInstance();
    rqueueMessageEnqueuer.enqueueUniqueIn(
        notificationQueue, notification.getId(), notification, 1000L);
    Notification newNotification = Notification.newInstance();
    newNotification.setId(notification.getId());
    sleep(100);
    rqueueMessageEnqueuer.enqueueUniqueIn(
        notificationQueue, newNotification.getId(), newNotification, 1000L);
    waitFor(() -> getMessageCount(notificationQueue) == 0, 30_000, "notification to be sent");
    Notification notificationFromDb =
        consumedMessageStore.getMessage(newNotification.getId(), Notification.class);
    assertEquals(newNotification, notificationFromDb);
  }
}
