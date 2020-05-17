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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MessageChannelTest extends SpringTestBase {
  private int messageCount = 200;
  /**
   * This test verified whether any pending message in the delayed queue are moved or not Whenever a
   * delayed message is pushed then it's checked whether there're any pending messages on delay
   * queue. if expired delayed messages are found on the head then a message is published on delayed
   * channel.
   */
  protected void verifyPublishMessageIsTriggeredOnMessageAddition() throws TimedOutException {
    String delayedQueueName = rqueueConfig.getDelayedQueueName(emailQueue);
    enqueueIn(delayedQueueName, i -> Email.newInstance(), i -> -1000L, messageCount);
    Email email = Email.newInstance();
    log.info("adding new message {}", email);
    messageSender.enqueueIn(emailQueue, email, Duration.ofMillis(1000));
    waitFor(
        () -> stringRqueueRedisTemplate.getZsetSize(delayedQueueName) <= 1,
        "one or zero messages in zset");
    assertTrue(
        "Messages are correctly moved",
        stringRqueueRedisTemplate.getListSize(rqueueConfig.getQueueName(emailQueue))
            >= messageCount);
    assertEquals(messageCount + 1L, messageSender.getAllMessages(emailQueue).size());
  }

  protected void verifyPublishMessageIsTriggeredOnMessageRemoval() throws TimedOutException {
    String processingQueueName = jobQueue;
    List<Job> jobs = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    int maxDelay = 2000;
    for (int i = 0; i < messageCount; i++) {
      Job job = Job.newInstance();
      jobs.add(job);
      ids.add(job.getId());
      int delay = random.nextInt(maxDelay);
      if (random.nextBoolean()) {
        delay = delay * -1;
      }
      enqueueIn(job, processingQueueName, delay);
    }
    TimeoutUtils.sleep(maxDelay);
    waitFor(
        () -> 0 == messageSender.getAllMessages(jobQueue).size(),
        30 * Constants.ONE_MILLI,
        "messages to be consumed");
    waitFor(
        () -> messageCount == consumedMessageService.getMessages(ids, Job.class).size(),
        "message count to be matched");
    waitFor(
        () -> jobs.containsAll(consumedMessageService.getMessages(ids, Job.class).values()),
        "All jobs to be executed");
  }
}
