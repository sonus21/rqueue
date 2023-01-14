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

import com.github.sonus21.junit.LocalTest;
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.common.SpringTestBase;
import com.github.sonus21.rqueue.test.dto.Sms;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.job.durability.in-terminal-state=0",
        "rqueue.job.enabled=false",
        "rqueue.retry.per.poll=1",
        "spring.data.redis.port=8022",
        "job.queue.active=false",
        "notification.queue.active=false",
        "email.queue.active=false",
        "sms.queue.concurrency=20-40",
        "sms.queue.active=true",
    })
@SpringBootIntegrationTest
@LocalTest
class ListenerConcurrencyTest extends SpringTestBase {

  @Test
  void runConcurrentTest() {
    int msgCount = 100_000;
    long executionTime = 10 * Constants.ONE_MILLI;
    long msgStartAt = System.currentTimeMillis();
    enqueue(smsQueue, (i) -> Sms.newInstance(), msgCount, false);
    long msgEndAt = System.currentTimeMillis();
    log.info("Msg enqueue time {}Ms", msgEndAt - msgStartAt);
    TimeoutUtils.sleep(executionTime);
    log.info("Enqueued msg count {}", msgCount);
    log.info("Approx remaining msgs {}", getMessageCount(smsQueue));
    log.info("Approx msg consumed {}", msgCount - getMessageCount(smsQueue));
  }
}
