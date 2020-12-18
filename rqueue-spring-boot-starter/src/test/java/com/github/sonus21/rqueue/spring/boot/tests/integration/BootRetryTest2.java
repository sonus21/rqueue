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
import com.github.sonus21.rqueue.spring.boot.application.ApplicationWithTaskExecutionBackoff;
import com.github.sonus21.rqueue.test.tests.RetryTests;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ExtendWith(SpringTestTracerExtension.class)
@ContextConfiguration(classes = ApplicationWithTaskExecutionBackoff.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
      "rqueue.retry.per.poll=1",
      "email.queue.retry.count=3",
      "spring.redis.port=8017",
      "mysql.db.name=BootRetryTest2",
      // 1500 ms
      "email.execution.time=1500",
      "use.system.redis=false",
      "fixed.backoff.interval=100",
    })
public class BootRetryTest2 extends RetryTests {
  @Test
  public void verifyMessageIsConsumedOnNextPoll() throws TimedOutException {
    verifyMultipleJobExecution();
  }
}
