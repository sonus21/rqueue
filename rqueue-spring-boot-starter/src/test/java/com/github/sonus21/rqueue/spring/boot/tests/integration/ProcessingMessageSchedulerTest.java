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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithCustomConfiguration;
import com.github.sonus21.rqueue.test.dto.Job;
import com.github.sonus21.rqueue.test.tests.SpringTestBase;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.test.RqueueSpringTestRunner;
import com.github.sonus21.test.RunTestUntilFail;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest
@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = ApplicationWithCustomConfiguration.class)
@Slf4j
@TestPropertySource(
    properties = {
      "rqueue.scheduler.auto.start=false",
      "spring.redis.port=6389",
      "mysql.db.name=test3",
      "max.workers.count=120",
      "use.system.redis=false"
    })
public class ProcessingMessageSchedulerTest extends SpringTestBase {
  @Rule
  public RunTestUntilFail retry =
      new RunTestUntilFail(
          log,
          () -> {
            for (Entry<String, List<RqueueMessage>> entry :
                getMessageMap(jobQueueName).entrySet()) {
              log.error("FAILING Queue {}", entry.getKey());
              for (RqueueMessage message : entry.getValue()) {
                log.error("FAILING Queue {} Msg {}", entry.getKey(), message);
              }
            }
          });

  private int messageCount = 110;

  @Test
  public void publishMessageIsTriggeredOnMessageRemoval() throws TimedOutException {
    String processingQueueName = jobQueueName;
    long currentTime = System.currentTimeMillis();
    List<Job> jobs = new ArrayList<>();
    List<String> ids = new ArrayList<>();
    int maxDelay = 2000;
    Random random = new Random();
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
        () -> 0 == messageSender.getAllMessages(jobQueueName).size(),
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
