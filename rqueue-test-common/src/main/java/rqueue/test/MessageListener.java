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

package rqueue.test;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import rqueue.test.dto.Email;
import rqueue.test.dto.Job;
import rqueue.test.dto.Notification;
import rqueue.test.service.ConsumedMessageService;
import rqueue.test.service.FailureManager;

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Transactional
@Slf4j
public class MessageListener {
  @NonNull private ConsumedMessageService consumedMessageService;
  @NonNull private FailureManager failureManager;

  @RqueueListener("${job.queue.name}")
  public void onMessage(Job job) throws Exception {
    if (failureManager.shouldFail(job.getId())) {
      throw new Exception("Failing job task to be retried" + job);
    }
    log.info("Job: {}", job);
    consumedMessageService.save(job);
  }

  @RqueueListener(
      value = "${notification.queue.name}",
      numRetries = "${notification.queue.retry.count}",
      delayedQueue = "true")
  public void onMessage(Notification notification) throws Exception {
    if (failureManager.shouldFail(notification.getId())) {
      throw new Exception("Failing notification task to be retried" + notification);
    }
    log.info("Notification: {}", notification);
    consumedMessageService.save(notification);
  }

  @RqueueListener(
      value = "${email.queue.name}",
      deadLetterQueue = "${email.dead.letter.queue.name}",
      numRetries = "${email.queue.retry.count}",
      delayedQueue = "true")
  public void onMessage(Email email) throws Exception {
    if (failureManager.shouldFail(email.getId())) {
      throw new Exception("Failing email task to be retried" + email);
    }
    log.info("Email: {}", email);
    consumedMessageService.save(email);
  }
}
