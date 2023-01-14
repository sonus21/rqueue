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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithMessageProcessor;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.tests.RetryTests;
import com.github.sonus21.rqueue.test.util.TestMessageProcessor;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.RetryingTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = ApplicationWithMessageProcessor.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1000",
        "rqueue.retry.per.poll=1000",
        "spring.data.redis.port=8023",
        "reservation.request.dead.letter.consumer.enabled=true",
        "reservation.request.active=true",
        "list.email.queue.enabled=true",
        "mysql.db.name=MessageProcessorTest",
        "use.system.redis=false",
        "user.banned.queue.active=true",
    })
@SpringBootIntegrationTest
class MessageProcessorTest extends RetryTests {

  @Autowired
  @Qualifier("preExecutionMessageProcessor")
  private TestMessageProcessor preExecutionMessageProcessor;

  @Autowired
  @Qualifier("postExecutionMessageProcessor")
  private TestMessageProcessor postExecutionMessageProcessor;

  @Autowired
  @Qualifier("manualDeletionMessageProcessor")
  private TestMessageProcessor manualDeletionMessageProcessor;

  @Autowired
  @Qualifier("deadLetterQueueMessageProcessor")
  private TestMessageProcessor deadLetterQueueMessageProcessor;

  @Autowired
  @Qualifier("discardMessageProcessor")
  private TestMessageProcessor discardMessageProcessor;

  @BeforeEach
  public void clean() {
    deadLetterQueueMessageProcessor.clear();
    preExecutionMessageProcessor.clear();
    postExecutionMessageProcessor.clear();
    manualDeletionMessageProcessor.clear();
    discardMessageProcessor.clear();
  }

  @Test
  void messageMovedToDeadLetterQueue() throws TimedOutException {
    verifyMessageMovedToDeadLetterQueue();
    assertEquals(3, preExecutionMessageProcessor.count());
    assertEquals(1, deadLetterQueueMessageProcessor.count());
    assertEquals(0, postExecutionMessageProcessor.count());
    assertEquals(0, manualDeletionMessageProcessor.count());
    assertEquals(0, discardMessageProcessor.count());
  }

  @Test
  void messageIsDiscardedAfterRetries() throws TimedOutException {
    verifyMessageIsDiscardedAfterRetries();
    assertEquals(3, preExecutionMessageProcessor.count());
    assertEquals(1, discardMessageProcessor.count());
    assertEquals(0, deadLetterQueueMessageProcessor.count());
    assertEquals(0, postExecutionMessageProcessor.count());
    assertEquals(0, manualDeletionMessageProcessor.count());
  }

  @Test
  void simpleMessageExecution() throws TimedOutException {
    verifySimpleTaskExecution();
    assertEquals(1, preExecutionMessageProcessor.count());
    assertEquals(1, postExecutionMessageProcessor.count());
    assertEquals(0, deadLetterQueueMessageProcessor.count());
    assertEquals(0, manualDeletionMessageProcessor.count());
    assertEquals(0, discardMessageProcessor.count());
  }

  @RetryingTest(2)
  void manualDeletionMessageProcessorTest() throws TimedOutException {
    cleanQueue(notificationQueue);
    Notification notification = Notification.newInstance();
    failureManager.createFailureDetail(notification.getId(), -1, 3);
    String messageId =
        enqueueAtGetMessageId(notificationQueue, notification, System.currentTimeMillis() + 1000L);
    rqueueMessageManager.deleteMessage(notificationQueue, messageId);
    waitFor(
        () -> {
          List<Object> messages = getAllMessages(notificationQueue);
          return !messages.contains(notification);
        },
        "message to be ignored");
    assertEquals(1, preExecutionMessageProcessor.count());
    assertEquals(1, manualDeletionMessageProcessor.count());
    assertEquals(0, postExecutionMessageProcessor.count());
    assertEquals(0, deadLetterQueueMessageProcessor.count());
    assertEquals(0, discardMessageProcessor.count());
  }
}
