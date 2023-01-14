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
import static com.google.common.collect.Lists.newArrayList;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.metrics.RqueueQueueMetrics;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.dto.Notification;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class MetricTest extends SpringTestBase {

  @Autowired
  protected MeterRegistry meterRegistry;
  @Autowired
  protected RqueueQueueMetrics rqueueQueueMetrics;

  protected void verifyScheduledQueueStatus() throws TimedOutException {
    long maxDelay = 0;
    int maxMessages = 100;
    for (int i = 0; i < maxMessages; i++) {
      long delay = random.nextInt(10000);
      if (maxDelay < delay) {
        maxDelay = delay;
      }
      Notification notification = Notification.newInstance();
      if (i < maxMessages / 2) {
        enqueueIn(notification, rqueueConfig.getScheduledQueueName(notificationQueue), -delay);
      } else {
        enqueueAt(notificationQueue, notification, Instant.now().plusMillis(delay));
      }
    }

    if (maxDelay == 5000) {
      Notification notification = Notification.newInstance();
      enqueueWithRetry(notificationQueue, notification, 10000);
    }

    waitFor(
        () ->
            meterRegistry
                .get("scheduled.queue.size")
                .tag("rqueue", "test")
                .tag("queue", notificationQueue)
                .gauge()
                .value()
                > 0,
        60000,
        "stats collection");
    waitFor(
        () ->
            meterRegistry
                .get("queue.size")
                .tag("rqueue", "test")
                .tag("queue", notificationQueue)
                .gauge()
                .value()
                > 0,
        60000,
        "Message in original queue");
    deleteAllMessages(notificationQueue);
    waitFor(() -> getMessageCount(notificationQueue) == 0, 60000, "notification queue to drain");
  }

  protected void verifyMetricStatus() throws TimedOutException {
    enqueue(emailDeadLetterQueue, i -> Email.newInstance(), 10, true);

    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), -1, 0);
    enqueue(jobQueue, job);

    waitFor(
        () ->
            meterRegistry
                .get("dead.letter.queue.size")
                .tags("rqueue", "test")
                .tags("queue", emailQueue)
                .gauge()
                .value()
                == 10,
        "stats collection");
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
    enqueue(emailQueue, Email.newInstance());
    Job job = Job.newInstance();
    failureManager.createFailureDetail(job.getId(), 1, 1);
    enqueue(jobQueue, job);
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

    waitFor(
        () ->
            meterRegistry
                .get("failure.count")
                .tags("rqueue", "test")
                .tags("queue", emailQueue)
                .counter()
                .count()
                == 0,
        "stats collection");
  }

  protected void verifyNotificationQueueMessageCount() throws TimedOutException {
    QueueDetail queueDetail = EndpointRegistry.get(notificationQueue);
    enqueue(queueDetail.getQueueName(), i -> Notification.newInstance(), 1000, true);
    enqueueIn(
        queueDetail.getScheduledQueueName(),
        i -> Notification.newInstance(),
        i -> 30_000L,
        100,
        true);
    TimeoutUtils.waitFor(
        () -> rqueueQueueMetrics.getProcessingMessageCount(notificationQueue) > 0,
        "at least one message in processing");
    TimeoutUtils.waitFor(
        () -> rqueueQueueMetrics.getScheduledMessageCount(notificationQueue) > 0,
        "at least one message in scheduled queue");
    TimeoutUtils.waitFor(
        () -> rqueueQueueMetrics.getPendingMessageCount(notificationQueue) > 0,
        "at least one message in pending queue");
  }

  protected void verifySmsQueueMessageCount() throws TimedOutException {
    String priority = "critical";
    QueueDetail queueDetail = EndpointRegistry.get(smsQueue, priority);
    enqueue(queueDetail.getQueueName(), i -> Sms.newInstance(), 1000, true);
    enqueueIn(queueDetail.getScheduledQueueName(), i -> Sms.newInstance(), i -> 30_000L, 100, true);
    TimeoutUtils.waitFor(
        () -> rqueueQueueMetrics.getProcessingMessageCount(smsQueue, priority) > 0,
        "at least one message in processing");
    TimeoutUtils.waitFor(
        () -> rqueueQueueMetrics.getScheduledMessageCount(smsQueue, priority) > 0,
        "at least one message in scheduled queue");
    TimeoutUtils.waitFor(
        () -> rqueueQueueMetrics.getPendingMessageCount(smsQueue, priority) > 0,
        "at least one message in pending queue");
  }
}
