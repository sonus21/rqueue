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
import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.utils.Constants;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class MetricTest extends SpringTestBase {
  @Autowired protected MeterRegistry meterRegistry;

  protected void verifyDelayedQueueStatus() throws TimedOutException {
    long maxDelay = 0;
    int maxMessages = 100;
    for (int i = 0; i < maxMessages; i++) {
      long delay = random.nextInt(10000);
      if (maxDelay < delay) {
        maxDelay = delay;
      }
      Notification notification = Notification.newInstance();
      if (i < maxMessages / 2) {
        enqueueIn(notification, rqueueConfig.getDelayedQueueName(notificationQueue), -delay);
      } else {
        messageSender.enqueueAt(notificationQueue, notification, Instant.now().plusMillis(delay));
      }
    }

    if (maxDelay == 5000) {
      Notification notification = Notification.newInstance();
      messageSender.enqueueWithRetry(notificationQueue, notification, 10000);
    }

    assertTrue(
        meterRegistry
                .get("delayed.queue.size")
                .tag("rqueue", "test")
                .tag("queue", notificationQueue)
                .gauge()
                .value()
            > 0);
    waitFor(
        () ->
            meterRegistry
                    .get("queue.size")
                    .tag("rqueue", "test")
                    .tag("queue", notificationQueue)
                    .gauge()
                    .value()
                > 0,
        "Message in original queue");
    messageSender.deleteAllMessages(notificationQueue);
    waitFor(
        () -> messageSender.getAllMessages(notificationQueue).size() == 0,
        "notification queue to drain");
  }

  protected void verifyMetricStatus() throws TimedOutException {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 10);

    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), -1, 0);
    messageSender.enqueue(jobQueue, job);

    assertEquals(
        10,
        meterRegistry
            .get("dead.letter.queue.size")
            .tags("rqueue", "test")
            .tags("queue", emailQueue)
            .gauge()
            .value(),
        0);
    waitFor(
        () ->
            meterRegistry
                    .get("processing.queue.size")
                    .tags("rqueue", "test")
                    .tags("queue", jobQueue)
                    .gauge()
                    .value()
                == 1,
        30 * Constants.ONE_MILLI,
        "processing queue message");
  }

  protected void verifyCountStatus() throws TimedOutException {
    messageSender.enqueue(emailQueue, Email.newInstance());
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 1, 1);
    messageSender.enqueue(jobQueue, job);
    waitFor(
        () ->
            meterRegistry
                    .get("failure.count")
                    .tags("rqueue", "test")
                    .tags("queue", jobQueue)
                    .counter()
                    .count()
                >= 1,
        30000,
        "job process",
        () -> printQueueStats(newArrayList(jobQueue, emailQueue, notificationQueue)));
    waitFor(
        () ->
            meterRegistry
                    .get("execution.count")
                    .tags("rqueue", "test")
                    .tags("queue", emailQueue)
                    .counter()
                    .count()
                == 1,
        "message process",
        () -> printQueueStats(newArrayList(jobQueue, emailQueue, notificationQueue)));

    assertEquals(
        0,
        meterRegistry
            .get("failure.count")
            .tags("rqueue", "test")
            .tags("queue", emailQueue)
            .counter()
            .count(),
        0);
  }
}
