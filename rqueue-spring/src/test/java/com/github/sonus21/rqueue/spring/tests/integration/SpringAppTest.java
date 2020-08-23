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

package com.github.sonus21.rqueue.spring.tests.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.test.tests.AllQueueMode;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.test.RqueueSpringTestRunner;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;

@ContextConfiguration(classes = SpringApp.class)
@RunWith(RqueueSpringTestRunner.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
      "spring.redis.port=7004",
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
public class SpringAppTest extends AllQueueMode {

  @Test
  public void numActiveQueues() {
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
  public void verifySimpleQueue() throws TimedOutException {
    testSimpleConsumer();
  }

  @Test
  public void verifyQueueLevelConsumer() throws TimedOutException {
    checkQueueLevelConsumer();
  }

  @Test
  public void verifyGroupConsumer() throws TimedOutException {
    checkGroupConsumer();
  }

  @Test
  public void verifyDefaultDeadLetterQueueRetry() throws TimedOutException {
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), 3, 10);
    enqueue(emailQueue, email);
    TimeoutUtils.waitFor(() -> getMessageCount(emailQueue) == 0, "email to be consumed");
    List<RqueueMessage> messages = rqueueMessageTemplate.readFromList(emailDeadLetterQueue, 0, -1);
    assertEquals(1, messages.size());
    Email email1 =
        (Email)
            MessageUtils.convertMessageToObject(
                messages.get(0), rqueueMessageSender.getMessageConverter());
    assertEquals(email.getId(), email1.getId());
  }

  @Test(expected = QueueDoesNotExist.class)
  public void testQueueDoesNotExist() {
    assertTrue(enqueue("job-push", Email.newInstance()));
  }

  @Test
  public void testOnlyPushMode() {
    Date date = Date.from(Instant.now().plusMillis(1000));
    registerQueue("job-push");
    registerQueue("sms-push", "critical", "high", "low");

    assertTrue(enqueue("job-push", Email.newInstance()));
    assertTrue(enqueueAt("job-push", Email.newInstance(), date));
    assertTrue(enqueue("sms-push", Sms.newInstance()));
    assertTrue(enqueueAt("sms-push", Sms.newInstance(), date.toInstant()));
    assertTrue(enqueueWithPriority("sms-push", "critical", Sms.newInstance()));
    assertTrue(enqueueAtWithPriority("sms-push", "critical", Sms.newInstance(), date));
    assertTrue(enqueueAtWithPriority("sms-push", "high", Sms.newInstance(), date.toInstant()));
    assertTrue(
        enqueueAtWithPriority(
            "sms-push", "low", Sms.newInstance(), date.toInstant().toEpochMilli()));
    assertEquals(
        8,
        getMessageCount(
            Arrays.asList(
                "job-push", "sms-push", "sms-push_critical", "sms-push_high", "sms-push_low")));
  }
}
