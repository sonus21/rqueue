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

package com.github.sonus21.rqueue.spring.tests.integration;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.app.SpringApp;
import com.github.sonus21.rqueue.test.tests.GroupPriorityTest;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.web.WebAppConfiguration;

@ContextConfiguration(classes = SpringApp.class)
@ExtendWith(RqueueSpringTestRunner.class)
@Slf4j
@WebAppConfiguration
@TestPropertySource(
    properties = {
      "spring.redis.port=7010",
      "mysql.db.name=WeightedPriorityQueueListener",
      "sms.queue.active=false",
      "notification.queue.active=false",
      "email.queue.active=false",
      "job.queue.active=false",
      "use.system.redis=false",
      "priority.mode=WEIGHTED",
      "reservation.queue.active=true",
      "feed.generation.queue.active=true",
      "chat.indexing.queue.active=true"
    })
public class WeightedPriorityQueueListener extends GroupPriorityTest {
  @Test
  public void simple() throws TimedOutException {
    checkGroupConsumer();
  }
}
