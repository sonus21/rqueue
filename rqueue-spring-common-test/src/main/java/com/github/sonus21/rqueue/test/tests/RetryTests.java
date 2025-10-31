/*
 * Copyright (c) 2019-2023 Sonu Kumar
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.ReservationRequest;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryTests extends SpringTestBase {

  protected void verifyAfterNRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    cleanQueue(jobQueue);
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 3, 10);
    rqueueMessageEnqueuer.enqueue(jobQueue, job);
    waitFor(
        () -> {
          Job jobInDb = consumedMessageStore.getMessage(job.getId(), Job.class);
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
    rqueueMessageEnqueuer.enqueueIn(emailQueue, email, 1000L);
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

  public void verifyRetry() throws TimedOutException {
    cleanQueue(emailQueue);
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), emailRetryCount - 1, emailRetryCount - 1);
    enqueue(emailQueue, email);
    waitFor(
        () -> failureManager.getFailureCount(email.getId()) == emailRetryCount - 1,
        "email task needs to be retried");
    waitFor(() -> getMessageCount(emailQueue) == 0, "job to be executed");
    Email emailInDb = consumedMessageStore.getMessage(email.getId(), Email.class);
    assertEquals(email, emailInDb);
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
    assertEquals(1, getMessageCount(jobQueue));
  }

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

  public void verifyMessageIsConsumedByDeadLetterQueueListener()
      throws TimedOutException, JsonProcessingException {
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
              consumedMessageStore.getMessage(request.getId(), ReservationRequest.class);
          return request.equals(requestInDb);
        },
        30000,
        "ReservationRequest to be run");
    ConsumedMessage consumedMessage = consumedMessageStore.getConsumedMessage(request.getId());
    assertNotNull(consumedMessage.getTag());
    assertNotEquals("", consumedMessage.getTag());
    RqueueMessage rqueueMessage =
        objectMapper.readValue(consumedMessage.getTag(), RqueueMessage.class);
    assertEquals(0, rqueueMessage.getFailureCount());
    assertEquals(reservationRequestQueueRetryCount, rqueueMessage.getSourceQueueFailureCount());
    assertEquals(reservationRequestQueue, rqueueMessage.getSourceQueueName());
    assertEquals(reservationRequestDeadLetterQueue, rqueueMessage.getQueueName());
    // message should not be added to list
    assertEquals(0L, stringRqueueRedisTemplate.getListSize(reservationRequestDeadLetterQueue));
    // no other messages are in queue
    assertEquals(0, getMessageCount(reservationRequestQueue));
    assertEquals(0, getMessageCount(reservationRequestDeadLetterQueue));
  }

  // this test verifies whether messages are moved from scheduled queue to main queue
  protected void verifyMultipleJobExecution() throws TimedOutException {
    assertEquals(1, retryPerPoll, "Retry per poll must be 1 to run this test");
    cleanQueue(emailQueue);
    Email email = Email.newInstance();
    failureManager.createFailureDetail(email.getId(), emailRetryCount - 1, emailRetryCount - 1);
    enqueue(emailQueue, email);
    waitFor(
        () -> failureManager.getFailureCount(email.getId()) == emailRetryCount - 1,
        "email task needs to be retried");
    waitFor(() -> getMessageCount(emailQueue) == 0, "job to be executed");
    List<ConsumedMessage> consumedMessages =
        consumedMessageStore.getConsumedMessages(email.getId());
    assertTrue(consumedMessages.size() > 1);
    log.info("Number of executions {}", consumedMessages.size());
  }
}
