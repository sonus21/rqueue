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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithCustomConfiguration;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.PeriodicJob;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ContextConfiguration(classes = ApplicationWithCustomConfiguration.class)
@Slf4j
@TestPropertySource(
    properties = {
        "spring.data.redis.port=8013",
        "mysql.db.name=PeriodicMessageTest",
        "rqueue.metrics.count.failure=false",
        "rqueue.metrics.count.execution=false",
        "periodic.job.queue.active=true",
        "use.system.redis=false",
        "monitor.enabled=false"
    })
@SpringBootIntegrationTest
class PeriodicMessageTest extends SpringTestBase {

  @Test
  void simplePeriodicMessage() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId =
        rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, Duration.ofSeconds(2));
    TimeoutUtils.waitFor(
        () -> {
          printQueueStats(periodicJobQueue);
          printConsumedMessage(periodicJobQueue);
          return consumedMessageStore.getConsumedMessages(job.getId()).size() > 1;
        },
        30_000,
        "at least two execution");
    rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
  }

  @Test
  void periodicMessageWithTimeUnit() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId =
        rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, 2000, TimeUnit.MILLISECONDS);
    TimeoutUtils.waitFor(
        () -> consumedMessageStore.getConsumedMessages(job.getId()).size() > 1,
        30_000,
        "at least two execution");
    rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
  }

  @Test
  void periodicMessageMilliseconds() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId = rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, 2000);
    TimeoutUtils.waitFor(
        () -> consumedMessageStore.getConsumedMessages(job.getId()).size() > 1,
        30_000,
        "at least two execution");
    rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
  }


  @Test
  void testPeriodicMessageDelete() throws TimedOutException, InterruptedException {
    rqueueEventListener.clearQueue();
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId = rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, 1000);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    List<Integer> l = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    executor.submit(new Runnable() {
      @SneakyThrows
      @Override
      public void run() {
        TimeoutUtils.waitFor(
            () -> consumedMessageStore.getConsumedMessageCount(job.getId()) > 1,
            30_000,
            "at least two execution");
        rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
        l.add(consumedMessageStore.getConsumedMessageCount(job.getId()));
        l.add(rqueueEventListener.getEventCount());
        latch.countDown();
      }
    });
    latch.await();
    executor.shutdown();
    // if task was running than next task should not schedule and run
    TimeoutUtils.sleep(5 * Constants.ONE_MILLI);
    assertEquals(l.get(0), consumedMessageStore.getConsumedMessageCount(job.getId()));
    assertTrue(
        // already scheduled job
        1 + l.get(1) == rqueueEventListener.getEventCount() ||
            // deleted just now, so no future scheduling
            l.get(1) == rqueueEventListener.getEventCount(),
        () -> String.format("Event Count does not match %d %d", rqueueEventListener.getEventCount(),
            l.get(1))
    );
  }
}
