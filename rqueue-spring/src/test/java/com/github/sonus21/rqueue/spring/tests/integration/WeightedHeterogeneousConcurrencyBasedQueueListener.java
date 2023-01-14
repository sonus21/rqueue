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

package com.github.sonus21.rqueue.spring.tests.integration;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.spring.tests.SpringIntegrationTest;
import com.github.sonus21.rqueue.test.tests.AllQueueMode;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;

@ContextConfiguration(classes = SpringApp.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
        "spring.data.redis.port=7014",
        "mysql.db.name=WeightedHeterogeneousConcurrencyBasedQueueListener",
        "sms.queue.active=true",
        "notification.queue.active=false",
        "email.queue.active=true",
        "job.queue.active=true",
        "priority.mode=WEIGHTED",
        "reservation.queue.active=true",
        "feed.generation.queue.active=true",
        "chat.indexing.queue.active=true",
        "sms.queue.concurrency=5",
        "reservation.queue.concurrency=2",
        "feed.generation.queue.concurrency=1-5",
        "chat.indexing.queue.concurrency=3-5"
    })
@SpringIntegrationTest
class WeightedHeterogeneousConcurrencyBasedQueueListener extends AllQueueMode {

  @Test
  void verifySimpleQueue() throws TimedOutException {
    testSimpleConsumer();
  }

  @Test
  void verifyQueueLevelConsumer() throws TimedOutException {
    checkQueueLevelConsumer();
  }

  @Test
  void verifyGroupConsumer() throws TimedOutException {
    checkGroupConsumer();
  }
}
