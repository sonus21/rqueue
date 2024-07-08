/*
 * Copyright (c) 2019-2024 Sonu Kumar
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

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.MessageListener;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.test.tests.RetryTests;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1",
        "spring.data.redis.port=6379",
        "reservation.request.dead.letter.consumer.enabled=true",
        "reservation.request.active=true",
        "list.email.queue.enabled=true",
        "mysql.db.name=BootDoNotSingleAttemptRetryTest",
        "record.failed.execution=true",
        "use.system.redis=true",
        "donot.retry=true"
    })
@SpringBootIntegrationTest
class BootDoNotRetrySingleAttemptTest extends RetryTests {

  @Test
  void taskIsNotRetried() throws TimedOutException {
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
    ConsumedMessage message = consumedMessageStore.getConsumedMessage(job.getId(),
        MessageListener.FAILED_TAG);
    assertEquals(1, message.getCount());
    List<ConsumedMessage> messages = consumedMessageStore.getAllMessages(job.getId());
    assertEquals(1, messages.size());
  }
}
