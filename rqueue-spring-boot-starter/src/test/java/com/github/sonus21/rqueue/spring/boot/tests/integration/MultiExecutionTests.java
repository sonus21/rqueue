/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithTaskExecutionBackoff;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.dto.LongRunningJob;
import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import com.github.sonus21.rqueue.test.tests.RetryTests;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = ApplicationWithTaskExecutionBackoff.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1",
        "email.queue.retry.count=3",
        "spring.data.redis.port=8017",
        "mysql.db.name=MultiExecutionTests",
        // 1500 ms
        "email.execution.time=1500",
        // 20 seconds is worker shutdown wait-time
        "long.running.job.queue.visibility.timeout=35000",
        "long.running.job.queue.active=true",
        "use.system.redis=false",
        "fixed.backoff.interval=100",
    })
@SpringBootIntegrationTest
class MultiExecutionTests extends RetryTests {

  @Test
  void verifyMessageIsConsumedOnNextPoll() throws TimedOutException {
    verifyMultipleJobExecution();
  }

  @Test
  @Disabled(value = "This test requires some work for restarting spring application...")
  void restartApplication() throws TimedOutException {
    LongRunningJob longRunningJob = LongRunningJob.newInstance(60000);
    enqueue(longRunningJobQueue, longRunningJob);
    waitFor(
        () -> getProcessingMessages(longRunningJobQueue).size() == 1,
        "message to be in processing queue");
    waitFor(
        () -> consumedMessageStore.getConsumedMessages(longRunningJob.getId()).size() == 1,
        "waiting for message to be consumed");
    ApplicationWithTaskExecutionBackoff.restart();
    TimeoutUtils.sleep(30_000);
    ConsumedMessage consumedMessage =
        consumedMessageStore.getConsumedMessage(longRunningJob.getId());
    String jobsKey = rqueueConfig.getJobsKey(consumedMessage.getTag());
    waitFor(
        () -> stringRqueueRedisTemplate.lrange(jobsKey, 0, -1).size() == 2,
        "message to be reprocessed");
  }
}
