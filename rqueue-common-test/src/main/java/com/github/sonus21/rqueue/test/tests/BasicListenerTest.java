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

package com.github.sonus21.rqueue.test.tests;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.ReservationRequest;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BasicListenerTest extends SpringTestBase {
  protected void verifyAfterNRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    cleanQueue(jobQueue);
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 3, 10);
    rqueueMessageSender.put(jobQueue, job);
    waitFor(
        () -> {
          Job jobInDb = consumedMessageService.getMessage(job.getId(), Job.class);
          return job.equals(jobInDb);
        },
        "job to be executed");
    waitFor(
        () -> {
          List<Object> messages = getAllMessages(jobQueue);
          return !messages.contains(job);
        },
        "message should be deleted from internal storage");
  }

  protected void verifyMessageMovedToDeadLetterQueue() throws TimedOutException {
    cleanQueue(emailQueue);
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), -1, 0);
    log.debug("queue: {} msg: {}", emailQueue, email);
    rqueueMessageSender.put(emailQueue, email, 1000L);
    waitFor(
        () -> emailRetryCount == failureManager.getFailureCount(email.getId()),
        30000000,
        "all retry to be exhausted");
    waitFor(
        () -> stringRqueueRedisTemplate.getListSize(emailDeadLetterQueue) > 0,
        "message should be moved to dead letter queue");
    assertEquals(emailRetryCount, failureManager.getFailureCount(email.getId()));
    failureManager.delete(email.getId());
  }

  protected void verifyMessageIsDiscardedAfterRetries() throws TimedOutException {
    cleanQueue(notificationQueue);
    Notification notification = Notification.newInstance();
    failureManager.createFailureDetail(notification.getId(), -1, notificationRetryCount);
    enqueueAt(notificationQueue, notification, System.currentTimeMillis() + 1000L);
    waitFor(
        () -> notificationRetryCount == failureManager.getFailureCount(notification.getId()),
        "all retry to be exhausted");
    waitFor(
        () -> {
          List<Object> messages = getAllMessages(notificationQueue);
          return !messages.contains(notification);
        },
        "message to be discarded");
    assertEquals(notificationRetryCount, failureManager.getFailureCount(notification.getId()));
  }

  public void verifySimpleTaskExecution() throws TimedOutException {
    cleanQueue(notificationQueue);
    Notification notification = Notification.newInstance();
    enqueue(notificationQueue, notification);
    waitFor(
        () -> {
          Notification notificationInDb =
              consumedMessageService.getMessage(notification.getId(), Notification.class);
          return notification.equals(notificationInDb);
        },
        "notification to be executed");
  }

  public void verifyRetry() throws TimedOutException {
    cleanQueue(emailQueue);
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), emailRetryCount - 1, emailRetryCount - 1);
    enqueue(emailQueue, email);
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

  public void verifyMessageIsInProcessingQueue() throws TimedOutException {
    cleanQueue(jobQueue);
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), -1, 0);
    enqueue(jobQueue, job);
    waitFor(() -> failureManager.getFailureCount(job.getId()) >= 3, "Job to be retried");
    waitFor(
        () -> {
          List<Object> messages = getAllMessages(jobQueue);
          return messages.contains(job);
        },
        "message should be present in internal storage");
    // more then one copy should not be present
    assertEquals(1, getAllMessages(jobQueue).size());
  }

  public void verifyMessageIsConsumedByDeadLetterQueueListener() throws TimedOutException {
    cleanQueue(reservationRequestQueue);
    cleanQueue(reservationRequestDeadLetterQueue);
    ReservationRequest request = ReservationRequest.newInstance();
    failureManager.createFailureDetail(
        request.getId(), reservationRequestQueueRetryCount, reservationRequestQueueRetryCount);
    enqueue(reservationRequestQueue, request);
    waitFor(
        () -> failureManager.getFailureCount(request.getId()) >= reservationRequestQueueRetryCount,
        60000,
        "ReservationRequest to be retried");
    waitFor(
        () -> {
          ReservationRequest requestInDb =
              consumedMessageService.getMessage(request.getId(), ReservationRequest.class);
          return request.equals(requestInDb);
        },
        30000,
        "ReservationRequest to be run");
    ConsumedMessage consumedMessage = consumedMessageService.getConsumedMessage(request.getId());
    assertEquals(consumedMessage.getTag(), "reservation-request-dlq");
    assertEquals(
        new Long(0), stringRqueueRedisTemplate.getListSize(reservationRequestDeadLetterQueue));
    assertEquals(0, getMessageCount(reservationQueue));
    assertEquals(0, getMessageCount(reservationRequestDeadLetterQueue));
  }

  protected void verifyListMessageListener() throws TimedOutException {
    int n = 1 + random.nextInt(10);
    List<Email> emails = new ArrayList<>();
    List<Email> delayedEmails = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      emails.add(Email.newInstance());
      delayedEmails.add(Email.newInstance());
    }
    enqueue(listEmailQueue, emails);
    enqueueIn(listEmailQueue, delayedEmails, 1, TimeUnit.SECONDS);
    TimeoutUtils.waitFor(
        () -> getMessageCount(listEmailQueue) == 0, "waiting for email list queue to drain");
    Collection<ConsumedMessage> messages =
        consumedMessageService.getConsumedMessages(
            emails.stream().map(Email::getId).collect(Collectors.toList()));
    Collection<ConsumedMessage> delayedMessages =
        consumedMessageService.getConsumedMessages(
            delayedEmails.stream().map(Email::getId).collect(Collectors.toList()));
    assertEquals(n, messages.size());
    assertEquals(n, delayedEmails.size());
    Set<String> delayedTags =
        delayedMessages.stream()
            .map(ConsumedMessage::getTag)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    Set<String> simpleTags =
        messages.stream()
            .map(ConsumedMessage::getTag)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    assertEquals(1, delayedTags.size());
    assertEquals(1, simpleTags.size());
  }
}
