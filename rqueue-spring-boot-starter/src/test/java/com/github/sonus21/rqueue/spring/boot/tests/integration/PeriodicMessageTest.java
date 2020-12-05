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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.junit.SpringTestTracerExtension;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithCustomConfiguration;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.PeriodicJob;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@ExtendWith(SpringTestTracerExtension.class)
@ContextConfiguration(classes = ApplicationWithCustomConfiguration.class)
@Slf4j
@TestPropertySource(
    properties = {
      "spring.redis.port=8013",
      "mysql.db.name=PeriodicMessageTest",
      "rqueue.metrics.count.failure=false",
      "rqueue.metrics.count.execution=false",
      "periodic.job.queue.active=true",
      "use.system.redis=false",
      "monitor.enabled=false"
    })
public class PeriodicMessageTest extends SpringTestBase {
  @Test
  public void testSimplePeriodicMessage() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId =
        rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, Duration.ofSeconds(2));
    TimeoutUtils.waitFor(
        () -> {
          printQueueStats(periodicJobQueue);
          printConsumedMessage(periodicJobQueue);
          return consumedMessageService.getConsumedMessages(job.getId()).size() > 1;
        },
        30_000,
        "at least two execution");
    rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
  }

  @Test
  public void testPeriodicMessageWithTimeUnit() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId =
        rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, 2000, TimeUnit.MILLISECONDS);
    TimeoutUtils.waitFor(
        () -> consumedMessageService.getConsumedMessages(job.getId()).size() > 1,
        30_000,
        "at least two execution");
    rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
  }

  @Test
  public void testPeriodicMessageMilliseconds() throws TimedOutException {
    PeriodicJob job = PeriodicJob.newInstance();
    String messageId = rqueueMessageEnqueuer.enqueuePeriodic(periodicJobQueue, job, 2000);
    TimeoutUtils.waitFor(
        () -> consumedMessageService.getConsumedMessages(job.getId()).size() > 1,
        30_000,
        "at least two execution");
    rqueueMessageManager.deleteMessage(periodicJobQueue, messageId);
  }
}
