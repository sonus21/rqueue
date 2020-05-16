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

package com.github.sonus21.rqueue.test;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.test.dto.ChatIndexing;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.FeedGeneration;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.Reservation;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.test.service.ConsumedMessageService;
import com.github.sonus21.rqueue.test.service.FailureManager;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Transactional
@Slf4j
public class MessageListener {
  @NonNull private ConsumedMessageService consumedMessageService;
  @NonNull private FailureManager failureManager;

  @RqueueListener(value = "${job.queue.name}", active = "${job.queue.active}")
  public void onMessage(Job job) throws Exception {
    log.info("Job: {}", job);
    if (failureManager.shouldFail(job.getId())) {
      throw new Exception("Failing job task to be retried" + job);
    }
    consumedMessageService.save(job);
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
    consumedMessageService.save(notification);
  }

  @RqueueListener(
      value = "${email.queue.name}",
      deadLetterQueue = "${email.dead.letter.queue.name}",
      numRetries = "${email.queue.retry.count}",
      visibilityTimeout = "${email.execution.time}",
      active = "${email.queue.active}")
  public void onMessage(Email email, @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage message)
      throws Exception {
    log.info("Email: {} Message: {}", email, message);
    if (failureManager.shouldFail(email.getId())) {
      throw new Exception("Failing email task to be retried" + email);
    }
    consumedMessageService.save(email);
  }

  @RqueueListener(
      value = "${sms.queue}",
      active = "${sms.queue.active}",
      priority = "${sms.queue.priority}",
      priorityGroup = "${sms.queue.group}",
      concurrency = "${sms.queue.concurrency}")
  public void onMessage(Sms sms) throws Exception {
    log.info("SmsListener: {}", sms);
    if (failureManager.shouldFail(sms.getId())) {
      throw new Exception("Failing sms task to be retried" + sms);
    }
    consumedMessageService.save(sms);
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
    consumedMessageService.save(chatIndexing);
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
    consumedMessageService.save(feedGeneration);
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
    consumedMessageService.save(reservation);
  }
}
