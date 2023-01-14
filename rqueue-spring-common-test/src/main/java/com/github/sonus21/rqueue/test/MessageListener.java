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

package com.github.sonus21.rqueue.test;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.test.dto.ChatIndexing;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.FeedGeneration;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.LongRunningJob;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.PeriodicJob;
import com.github.sonus21.rqueue.test.dto.Reservation;
import com.github.sonus21.rqueue.test.dto.ReservationRequest;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.test.service.ConsumedMessageStore;
import com.github.sonus21.rqueue.test.service.FailureManager;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import jakarta.annotation.PostConstruct;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

@com.github.sonus21.rqueue.annotation.MessageListener
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class MessageListener {

  @NonNull
  private final ConsumedMessageStore consumedMessageStore;
  @NonNull
  private final FailureManager failureManager;
  @NonNull
  private final RqueueConfig rqueueConfig;
  private ScheduledExecutorService scheduledExecutorService;

  @Value("${job.queue.name}")
  private String jobQueue;

  @Value("${notification.queue.name}")
  private String notificationQueueName;

  @Value("${email.queue.name}")
  private String emailQueue;

  @Value("${sms.queue}")
  private String smsQueue;

  @Value("${chat.indexing.queue}")
  private String chatIndexingQueue;

  @Value("${feed.generation.queue}")
  private String feedGenerationQueue;

  @Value("${reservation.queue}")
  private String reservationQueue;

  @Value("${reservation.request.queue.name}")
  private String reservationRequestQueue;

  @Value("${reservation.request.dead.letter.queue.name}")
  private String reservationRequestDeadLetterQueue;

  @Value("${list.email.queue.name}")
  private String listEmailQueue;

  @Value("${periodic.job.queue.name}")
  private String periodicJobQueue;

  @Value("${scheduled.job.executors.thead.count:1}")
  private int threadCount;

  @Value("${long.running.job.queue.name:}")
  private String longRunningJobQueue;

  @Value("${checkin.enabled:false}")
  private boolean checkinEnabled;

  @RqueueListener(value = "${job.queue.name}", active = "${job.queue.active}")
  public void onMessage(Job job) throws Exception {
    log.info("Job: {}", job);
    if (failureManager.shouldFail(job.getId())) {
      throw new Exception("Failing job task to be retried" + job);
    }
    consumedMessageStore.save(job, null, jobQueue);
  }

  @PostConstruct
  public void init() {
    if (rqueueConfig.isJobEnabled()) {
      scheduledExecutorService = new ScheduledThreadPoolExecutor(threadCount);
    }
  }

  @RqueueListener(
      value = "${notification.queue.name}",
      numRetries = "${notification.queue.retry.count}",
      active = "${notification.queue.active}")
  public void onMessage(
      @Payload Notification notification, @Header(RqueueMessageHeaders.ID) String id)
      throws Exception {
    log.info("Notification: {}, Id: {}", notification, id);
    if (failureManager.shouldFail(notification.getId())) {
      throw new Exception("Failing notification task to be retried" + notification);
    }
    consumedMessageStore.save(notification, null, notificationQueueName);
  }

  @RqueueListener(
      value = "${email.queue.name}",
      deadLetterQueue = "${email.dead.letter.queue.name}",
      numRetries = "${email.queue.retry.count}",
      visibilityTimeout = "${email.execution.time}",
      active = "${email.queue.active}")
  public void onMessage(
      Email email,
      @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage message,
      @Header(RqueueMessageHeaders.JOB) com.github.sonus21.rqueue.core.Job job)
      throws Exception {
    log.info("Email: {} Message: {}", email, message);
    consumedMessageStore.save(email, job.getId(), emailQueue);
    if (failureManager.shouldFail(email.getId())) {
      throw new Exception("Failing email task to be retried" + email);
    }
  }

  @RqueueListener(
      value = "${sms.queue}",
      active = "${sms.queue.active}",
      priority = "${sms.queue.priority}",
      priorityGroup = "${sms.queue.group}",
      concurrency = "${sms.queue.concurrency}",
      batchSize = "40")
  public void onMessage(Sms sms) throws Exception {
    log.info("SmsListener: {}", sms);
    if (failureManager.shouldFail(sms.getId())) {
      throw new Exception("Failing sms task to be retried" + sms);
    }
    consumedMessageStore.save(sms, null, smsQueue);
  }

  @RqueueListener(
      value = "${chat.indexing.queue}",
      active = "${chat.indexing.queue.active}",
      priority = "${chat.indexing.queue.priority}",
      priorityGroup = "${chat.indexing.queue.group}",
      concurrency = "${chat.indexing.queue.concurrency}")
  public void onMessage(ChatIndexing chatIndexing) throws Exception {
    log.info("ChatIndexing: {}", chatIndexing);
    if (failureManager.shouldFail(chatIndexing.getId())) {
      throw new Exception("Failing chat indexing task to be retried" + chatIndexing);
    }
    consumedMessageStore.save(chatIndexing, null, chatIndexingQueue);
  }

  @RqueueListener(
      value = "${feed.generation.queue}",
      active = "${feed.generation.queue.active}",
      priority = "${feed.generation.queue.priority}",
      priorityGroup = "${feed.generation.queue.group}",
      concurrency = "${feed.generation.queue.concurrency}")
  public void onMessage(FeedGeneration feedGeneration) throws Exception {
    log.info("FeedGeneration: {}", feedGeneration);
    if (failureManager.shouldFail(feedGeneration.getId())) {
      throw new Exception("Failing feedGeneration task to be retried" + feedGeneration);
    }
    consumedMessageStore.save(feedGeneration, null, feedGenerationQueue);
  }

  @RqueueListener(
      value = "${reservation.queue}",
      active = "${reservation.queue.active}",
      priority = "${reservation.queue.priority}",
      priorityGroup = "${reservation.queue.group}",
      concurrency = "${reservation.queue.concurrency}")
  public void onMessage(Reservation reservation) throws Exception {
    log.info("Reservation: {}", reservation);
    if (failureManager.shouldFail(reservation.getId())) {
      throw new Exception("Failing reservation task to be retried" + reservation);
    }
    consumedMessageStore.save(reservation, null, reservationQueue);
  }

  @RqueueListener(
      value = "${reservation.request.queue.name}",
      deadLetterQueue = "${reservation.request.dead.letter.queue.name}",
      deadLetterQueueListenerEnabled = "${reservation.request.dead.letter.consumer.enabled}",
      active = "${reservation.request.active}",
      numRetries = "${reservation.request.queue.retry.count}")
  public void onMessageReservationRequest(ReservationRequest request) throws Exception {
    log.info("ReservationRequest {}", request);
    if (failureManager.shouldFail(request.getId())) {
      throw new Exception("Failing reservation request task to be retried" + request);
    }
    consumedMessageStore.save(request, null, reservationRequestQueue);
  }

  @RqueueListener(
      value = "${reservation.request.dead.letter.queue.name}",
      active = "${reservation.request.dead.letter.consumer.enabled}",
      numRetries = "${reservation.request.dead.letter.queue.retry.count}")
  public void onMessageReservationRequestDeadLetterQueue(
      ReservationRequest request, @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage rqueueMessage)
      throws Exception {
    log.info("ReservationRequest Dead Letter Queue{}", request);
    consumedMessageStore.save(request, rqueueMessage, reservationRequestDeadLetterQueue);
  }

  @RqueueListener(value = "${list.email.queue.name}", active = "${list.email.queue.enabled}")
  public void onMessageEmailList(List<Email> emailList) throws JsonProcessingException {
    log.info("onMessageEmailList {}", emailList);
    String consumedId = UUID.randomUUID().toString();
    for (Email email : emailList) {
      consumedMessageStore.save(email, consumedId, listEmailQueue);
    }
  }

  @RqueueListener(
      value = "${periodic.job.queue.name}",
      active = "${periodic.job.queue.active}",
      deadLetterQueue = "${periodic.job.dead.letter.queue.name}",
      numRetries = "${periodic.job.queue.retry.count}")
  public void onPeriodicJob(
      PeriodicJob periodicJob,
      @Header(RqueueMessageHeaders.JOB) com.github.sonus21.rqueue.core.Job job)
      throws Exception {
    log.info("onPeriodicJob: {}", periodicJob);
    if (failureManager.shouldFail(periodicJob.getId())) {
      throw new Exception("Failing PeriodicJob task to be retried" + periodicJob);
    }
    consumedMessageStore.save(periodicJob, UUID.randomUUID().toString(), periodicJobQueue);
    if (checkinEnabled) {
      long endTime = System.currentTimeMillis() + 1000L * periodicJob.getExecutionTime();
      scheduledExecutorService.schedule(
          new CheckinClerk(scheduledExecutorService, job, endTime, 500L),
          10L,
          TimeUnit.MILLISECONDS);
      do {
        TimeoutUtils.sleep(200L);
      } while (System.currentTimeMillis() < endTime);
    }
  }

  @RqueueListener(
      value = "${long.running.job.queue.name:long-running-job-queue}",
      active = "${long.running.job.queue.active:false}",
      deadLetterQueue = "${long.running.job.dead.letter.queue.name:}",
      numRetries = "${long.running.job.queue.retry.count:-1}",
      visibilityTimeout = "${long.running.job.queue.visibility.timeout:90000}")
  public void onLongRunningJob(
      LongRunningJob longRunningJob,
      @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage rqueueMessage,
      @Header(RqueueMessageHeaders.JOB) com.github.sonus21.rqueue.core.Job job)
      throws Exception {
    log.info("onLongRunningJob: {}", longRunningJob);
    if (failureManager.shouldFail(longRunningJob.getId())) {
      throw new Exception("Failing LongRunningJob task to be retried" + longRunningJob);
    }
    long endTime = System.currentTimeMillis() + longRunningJob.getRunTime();
    scheduledExecutorService.schedule(
        new CheckinClerk(scheduledExecutorService, job, endTime, 500L), 10L, TimeUnit.MILLISECONDS);
    consumedMessageStore.save(longRunningJob, rqueueMessage.getId(), longRunningJobQueue);
    do {
      TimeoutUtils.sleep(200L);
    } while (System.currentTimeMillis() < endTime);
  }

  static class CheckinClerk implements Runnable {

    private final WeakReference<ScheduledExecutorService> serviceWeakReference;
    private final com.github.sonus21.rqueue.core.Job job;
    private final long endCheckInTime;
    private final long checkinInterval;
    private int checkInId = 1;

    CheckinClerk(
        ScheduledExecutorService scheduledExecutorService,
        com.github.sonus21.rqueue.core.Job job,
        long endCheckInTime,
        long checkinInterval) {
      this.endCheckInTime = endCheckInTime;
      this.job = job;
      this.checkinInterval = checkinInterval;
      this.serviceWeakReference = new WeakReference<>(scheduledExecutorService);
    }

    @Override
    public void run() {
      if (endCheckInTime < System.currentTimeMillis()) {
        return;
      }
      this.job.checkIn("Running ..." + checkInId);
      long startedAt = System.currentTimeMillis();
      Duration visibilityTimout = this.job.getVisibilityTimeout(); // 1000
      assertTrue(this.job.updateVisibilityTimeout(Duration.ofMillis(1000))); // 2000+
      Duration newVisibilityTimeout = this.job.getVisibilityTimeout(); // 2000+
      long delta = newVisibilityTimeout.minus(visibilityTimout).toMillis();
      long now = System.currentTimeMillis();
      assertTrue(delta >= (1000 - now - startedAt) && delta <= 1000);
      checkInId += 1;
      if (endCheckInTime > System.currentTimeMillis() + this.checkinInterval) {
        this.serviceWeakReference.get().schedule(this, this.checkinInterval, TimeUnit.MILLISECONDS);
      }
    }
  }
}
