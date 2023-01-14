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

package com.github.sonus21.rqueue.spring.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.spring.tests.SpringIntegrationTest;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.test.tests.AllQueueMode;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;

@ContextConfiguration(classes = SpringApp.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
        "spring.data.redis.port=7004",
        "mysql.db.name=SpringAppTest",
        "sms.queue.active=true",
        "notification.queue.active=false",
        "email.queue.active=true",
        "job.queue.active=true",
        "use.system.redis=false",
        "priority.mode=STRICT",
        "reservation.queue.active=true",
        "feed.generation.queue.active=true",
        "chat.indexing.queue.active=true",
        "provide.executor=true",
        "email.queue.retry.count=-1",
        "rqueue.retry.per.poll=10"
    })
@SpringIntegrationTest
class SpringAppTest extends AllQueueMode {

  @Test
  void numActiveQueues() {
    Map<String, QueueDetail> registeredQueue = EndpointRegistry.getActiveQueueMap();
    assertEquals(10, registeredQueue.size());
    assertFalse(registeredQueue.containsKey(notificationQueue));
    assertTrue(registeredQueue.containsKey(emailQueue));
    assertTrue(registeredQueue.containsKey(jobQueue));
    assertTrue(registeredQueue.containsKey(feedGenerationQueue));
    assertTrue(registeredQueue.containsKey(reservationQueue));
    assertTrue(registeredQueue.containsKey(smsQueue));
  }

  @Test
  void verifySimpleQueue() throws TimedOutException {
    testSimpleConsumer();
  }

  @Test
  void verifyQueueLevelConsumer() throws TimedOutException {
    checkQueueLevelConsumer();
  }

  @Test
  void verifyGroupConsumer() throws TimedOutException {
    checkGroupConsumer();
  }

  @Test
  void verifyDefaultDeadLetterQueueRetry() throws TimedOutException {
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), 3, 10);
    enqueue(emailQueue, email);
    TimeoutUtils.waitFor(() -> getMessageCount(emailQueue) == 0, "email to be consumed");
    List<RqueueMessage> messages = rqueueMessageTemplate.readFromList(emailDeadLetterQueue, 0, -1);
    assertEquals(1, messages.size());
    Email email1 =
        (Email)
            RqueueMessageUtils.convertMessageToObject(
                messages.get(0), rqueueMessageManager.getMessageConverter());
    assertEquals(email.getId(), email1.getId());
  }

  @Test
  void queueDoesNotExist() {
    assertThrows(QueueDoesNotExist.class, () -> enqueue("job-push-xyz", Email.newInstance()));
  }

  @Test
  void onlyPushMode() {
    Date date = Date.from(Instant.now().plusMillis(Constants.ONE_MILLI));
    String emailQueue = "email-push";
    String smsQueue = "sms-push";
    String high = "high";
    String low = "low";
    String critical = "critical";
    registerQueue(emailQueue);
    registerQueue(smsQueue, critical, high, low);
    Email[] emails =
        new Email[]{
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
            Email.newInstance(),
        };
    Sms[] sms =
        new Sms[]{
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
            Sms.newInstance(),
        };

    assertTrue(enqueue(emailQueue, emails[0]));
    assertTrue(rqueueMessageEnqueuer.enqueue(emailQueue, emails[1].getId(), emails[1]));
    assertTrue(rqueueMessageEnqueuer.enqueueUnique(emailQueue, emails[2].getId(), emails[2]));
    assertNotNull(rqueueMessageEnqueuer.enqueueWithRetry(emailQueue, emails[3], 3));
    assertTrue(rqueueMessageEnqueuer.enqueueWithRetry(emailQueue, emails[4].getId(), emails[4], 3));

    assertTrue(enqueueAt(emailQueue, emails[5], date));
    assertTrue(rqueueMessageEnqueuer.enqueueAt(emailQueue, emails[6].getId(), emails[6], date));

    assertTrue(enqueueAt(emailQueue, emails[7], Instant.now().plusMillis(Constants.ONE_MILLI)));
    assertTrue(
        rqueueMessageEnqueuer.enqueueAt(
            emailQueue, emails[8].getId(), emails[8], date.toInstant()));

    assertTrue(
        rqueueMessageEnqueuer.enqueueUniqueAt(
            emailQueue, emails[9].getId(), emails[9], date.toInstant().toEpochMilli()));

    assertTrue(
        enqueueAt(
            emailQueue, emails[10], Instant.now().plusMillis(Constants.ONE_MILLI).toEpochMilli()));
    assertTrue(
        rqueueMessageEnqueuer.enqueueAt(
            emailQueue,
            emails[11].getId(),
            emails[11],
            Instant.now().plusMillis(Constants.ONE_MILLI).toEpochMilli()));

    assertTrue(enqueueIn(emailQueue, emails[12], Constants.ONE_MILLI));
    assertTrue(
        rqueueMessageEnqueuer.enqueueIn(
            emailQueue, emails[13].getId(), emails[13], Constants.ONE_MILLI));

    assertTrue(enqueueIn(emailQueue, emails[14], Constants.ONE_MILLI, TimeUnit.MILLISECONDS));
    assertTrue(
        rqueueMessageEnqueuer.enqueueIn(
            emailQueue,
            emails[15].getId(),
            emails[15],
            Constants.ONE_MILLI,
            TimeUnit.MILLISECONDS));

    assertTrue(enqueueIn(emailQueue, emails[16], Duration.ofMillis(Constants.ONE_MILLI)));
    assertTrue(
        rqueueMessageEnqueuer.enqueueIn(
            emailQueue, emails[17].getId(), emails[17], Duration.ofMillis(Constants.ONE_MILLI)));

    assertTrue(
        rqueueMessageEnqueuer.enqueueUniqueIn(
            emailQueue, emails[18].getId(), emails[18], Constants.ONE_MILLI));

    assertNotNull(
        rqueueMessageEnqueuer.enqueueInWithRetry(emailQueue, emails[19], 3, Constants.ONE_MILLI));

    assertTrue(
        rqueueMessageEnqueuer.enqueueInWithRetry(
            emailQueue, emails[20].getId(), emails[20], 3, Constants.ONE_MILLI));

    assertNotNull(
        rqueueMessageEnqueuer.enqueuePeriodic(emailQueue, emails[21], Constants.ONE_MILLI));
    assertNotNull(
        rqueueMessageEnqueuer.enqueuePeriodic(
            emailQueue, emails[22], Constants.ONE_MILLI, TimeUnit.MILLISECONDS));
    assertNotNull(
        rqueueMessageEnqueuer.enqueuePeriodic(
            emailQueue, emails[23], Duration.ofMillis(Constants.ONE_MILLI)));

    assertTrue(
        rqueueMessageEnqueuer.enqueuePeriodic(
            emailQueue, emails[24].getId(), emails[24], Constants.ONE_MILLI));
    assertTrue(
        rqueueMessageEnqueuer.enqueuePeriodic(
            emailQueue,
            emails[25].getId(),
            emails[25],
            Constants.ONE_MILLI,
            TimeUnit.MILLISECONDS));
    assertTrue(
        rqueueMessageEnqueuer.enqueuePeriodic(
            emailQueue, emails[26].getId(), emails[26], Duration.ofMillis(Constants.ONE_MILLI)));

    assertTrue(enqueue(smsQueue, sms[0]));
    assertTrue(enqueueIn(smsQueue, sms[1], Constants.ONE_MILLI));

    assertTrue(enqueueWithPriority(smsQueue, critical, sms[2]));
    assertTrue(rqueueMessageEnqueuer.enqueueWithPriority(smsQueue, critical, sms[3].getId(), sms));

    assertTrue(enqueueWithPriority(smsQueue, critical, sms[4]));
    assertTrue(
        rqueueMessageEnqueuer.enqueueWithPriority(smsQueue, critical, sms[5].getId(), sms[5]));

    assertTrue(
        rqueueMessageEnqueuer.enqueueUniqueWithPriority(
            smsQueue, critical, sms[6].getId(), sms[6]));

    assertTrue(enqueueAtWithPriority(smsQueue, critical, sms[7], date));
    assertTrue(
        rqueueMessageEnqueuer.enqueueAtWithPriority(
            smsQueue, critical, sms[8].getId(), sms[8], date));

    assertTrue(enqueueAtWithPriority(smsQueue, high, sms[9], date.toInstant()));
    assertTrue(
        rqueueMessageEnqueuer.enqueueAtWithPriority(
            smsQueue, high, sms[10].getId(), sms[10], date.toInstant()));

    assertTrue(enqueueAtWithPriority(smsQueue, low, sms[11], date.toInstant().toEpochMilli()));
    assertTrue(
        rqueueMessageEnqueuer.enqueueAtWithPriority(
            smsQueue, low, sms[12].getId(), sms[12], date.toInstant().toEpochMilli()));
    assertTrue(
        rqueueMessageEnqueuer.enqueueUniqueAtWithPriority(
            smsQueue, critical, sms[13].getId(), sms[13], date.toInstant().toEpochMilli()));

    assertTrue(enqueueInWithPriority(smsQueue, critical, sms[14], Constants.ONE_MILLI));
    assertTrue(
        rqueueMessageEnqueuer.enqueueInWithPriority(
            smsQueue, critical, sms[15].getId(), sms[15], Constants.ONE_MILLI));

    assertTrue(
        enqueueInWithPriority(smsQueue, high, sms[16], Constants.ONE_MILLI, TimeUnit.MILLISECONDS));
    assertTrue(
        rqueueMessageEnqueuer.enqueueInWithPriority(
            smsQueue, high, sms[17].getId(), sms[17], Constants.ONE_MILLI, TimeUnit.MILLISECONDS));

    assertTrue(
        enqueueInWithPriority(smsQueue, low, sms[18], Duration.ofMillis(Constants.ONE_MILLI)));
    assertTrue(
        rqueueMessageEnqueuer.enqueueInWithPriority(
            smsQueue, low, sms[19].getId(), sms[19], Duration.ofMillis(Constants.ONE_MILLI)));

    assertTrue(
        rqueueMessageEnqueuer.enqueueUniqueInWithPriority(
            smsQueue, low, sms[20].getId(), sms[20], Constants.ONE_MILLI, TimeUnit.MILLISECONDS));

    assertEquals(
        48,
        getMessageCount(
            Arrays.asList(
                emailQueue,
                smsQueue,
                smsQueue + "_" + critical,
                smsQueue + "_" + high,
                smsQueue + "_" + low)));
  }
}
