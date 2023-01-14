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

package com.github.sonus21.rqueue.test.tests;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.UserBanned;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

@Slf4j
public abstract class BasicListenerTest extends SpringTestBase {

  public void verifySimpleTaskExecution() throws TimedOutException {
    cleanQueue(notificationQueue);
    Notification notification = Notification.newInstance();
    enqueue(notificationQueue, notification);
    waitFor(
        () -> {
          Notification notificationInDb =
              consumedMessageStore.getMessage(notification.getId(), Notification.class);
          return notification.equals(notificationInDb);
        },
        "notification to be executed");
  }

  protected void verifyListMessageListener() throws TimedOutException {
    int n = 1 + random.nextInt(10);
    List<Email> emails = new ArrayList<>();
    List<Email> scheduledEmails = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      emails.add(Email.newInstance());
      scheduledEmails.add(Email.newInstance());
    }
    enqueue(listEmailQueue, emails);
    enqueueIn(listEmailQueue, scheduledEmails, 1, TimeUnit.SECONDS);
    TimeoutUtils.waitFor(
        () -> getMessageCount(listEmailQueue) == 0, "waiting for email list queue to drain");
    Collection<ConsumedMessage> messages =
        consumedMessageStore.getConsumedMessages(
            emails.stream().map(Email::getId).collect(Collectors.toList()));
    Collection<ConsumedMessage> scheduledMessages =
        consumedMessageStore.getConsumedMessages(
            scheduledEmails.stream().map(Email::getId).collect(Collectors.toList()));
    assertEquals(n, messages.size());
    assertEquals(n, scheduledEmails.size());
    Set<String> scheduledTags =
        scheduledMessages.stream()
            .map(ConsumedMessage::getTag)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    Set<String> simpleTags =
        messages.stream()
            .map(ConsumedMessage::getTag)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    assertEquals(1, scheduledTags.size());
    assertEquals(1, simpleTags.size());
  }

  protected void verifyScheduledTaskExecution() throws TimedOutException {
    cleanQueue(jobQueue);
    Job job = Job.newInstance();
    enqueueIn(jobQueue, job, 1_000L);
    waitFor(
        () -> {
          List<Object> messages = getAllMessages(jobQueue);
          return messages.contains(job);
        },
        "message should be present in internal storage");
    waitFor(() -> getMessageCount(jobQueue) == 0, 30_000, "job to run");
  }

  protected void testMultiMessageConsumer() throws TimedOutException {
    enqueue(userBannedQueue, UserBanned.newInstance());
    enqueueIn(userBannedQueue, UserBanned.newInstance(), Duration.ofSeconds(1));
    TimeoutUtils.waitFor(
        () -> getMessageCount(userBannedQueue) == 0,
        20 * Constants.ONE_MILLI,
        "user banned queues to drain");
    TimeoutUtils.waitFor(
        () -> {
          List<ConsumedMessage> messageList =
              consumedMessageStore.getConsumedMessagesForQueue(userBannedQueue);
          if (CollectionUtils.isEmpty(messageList)) {
            return false;
          }
          if (random.nextBoolean()) {
            log.info("SIZE {}", messageList.size());
          }
          return messageList.size() == 8;
        },
        20 * Constants.ONE_MILLI,
        "waiting for all message consumer to save");
  }
}
