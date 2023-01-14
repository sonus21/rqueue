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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithCustomConfiguration;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.ChatIndexing;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.FeedGeneration;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.Sms;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ContextConfiguration(classes = ApplicationWithCustomConfiguration.class)
@Slf4j
@TestPropertySource(
    properties = {
        "spring.data.redis.port=8008",
        "mysql.db.name=MessageEnqueuerTest",
        "rqueue.metrics.count.failure=false",
        "rqueue.metrics.count.execution=false",
        "sms.queue.active=true",
        "sms.queue.group=sms-test",
        "notification.queue.active=true",
        "email.queue.active=true",
        "job.queue.active=true",
        "priority.mode=STRICT",
        "reservation.queue.active=true",
        "reservation.queue.group=",
        "feed.generation.queue.active=true",
        "feed.generation.queue.group=",
        "chat.indexing.queue.active=true",
        "chat.indexing.queue.group=",
        "sms.queue.concurrency=5",
        "reservation.queue.concurrency=2",
        "feed.generation.queue.concurrency=1-5",
        "chat.indexing.queue.concurrency=3-5"
    })
@SpringBootIntegrationTest
class MessageEnqueuerTest extends SpringTestBase {

  @Test
  void enqueueWithMessageId() throws TimedOutException {
    Email email = Email.newInstance();
    assertTrue(rqueueMessageEnqueuer.enqueue(emailQueue, email.getId(), email));
    waitFor(() -> getMessageCount(emailQueue) == 0, "email to be consumed");
  }

  @Test
  void enqueueWithRetryWithMessageId() throws TimedOutException {
    ChatIndexing chatIndexing = ChatIndexing.newInstance();
    assertTrue(
        rqueueMessageEnqueuer.enqueueWithRetry(
            chatIndexingQueue, chatIndexing.getId(), chatIndexing, 3));
    waitFor(() -> getMessageCount(chatIndexingQueue) == 0, "ChatIndexing to be done");
  }

  @Test
  void enqueueWithPriorityWithMessageId() throws TimedOutException {
    Sms chat = Sms.newInstance();
    assertTrue(rqueueMessageEnqueuer.enqueueWithPriority(smsQueue, "medium", chat.getId(), chat));
    waitFor(() -> getMessageCount(smsQueue, "medium") == 0, "notification to be consumed");
  }

  @Test
  void enqueueInWithMessageId() throws TimedOutException {
    Job job = Job.newInstance();
    assertTrue(rqueueMessageEnqueuer.enqueueIn(jobQueue, job.getId(), job, 1000));
    waitFor(() -> getMessageCount(jobQueue) == 0, "job notification to be sent");
  }

  @Test
  void enqueueInWithRetryWithMessageId() throws TimedOutException {
    Notification notification = Notification.newInstance();
    assertTrue(
        rqueueMessageEnqueuer.enqueueInWithRetry(
            notificationQueue, notification.getId(), notification, 3, 1000));
    waitFor(() -> getMessageCount(notificationQueue) == 0, "notification to be consumed");
  }

  @Test
  void enqueueInWithPriorityWithMessageId() throws TimedOutException {
    Sms sms = Sms.newInstance();
    assertTrue(
        rqueueMessageEnqueuer.enqueueInWithPriority(smsQueue, "high", sms.getId(), sms, 1000L));
    waitFor(() -> getMessageCount(smsQueue, "high") == 0, "sms to be sent");
  }

  @Test
  void enqueueAtWithMessageId() throws TimedOutException {
    FeedGeneration feedGeneration = FeedGeneration.newInstance();
    assertTrue(
        rqueueMessageEnqueuer.enqueueAt(
            feedGenerationQueue,
            feedGeneration.getId(),
            feedGeneration,
            System.currentTimeMillis() + 1000));
    waitFor(() -> getMessageCount(feedGenerationQueue) == 0, "Feed to be generated");
  }
}
