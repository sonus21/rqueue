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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.junit.LocalTest;
import com.github.sonus21.junit.TestRunner;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationListenerDisabled;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.tests.MessageChannelTests;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ContextConfiguration(classes = ApplicationListenerDisabled.class)
@TestPropertySource(
    properties = {
        "rqueue.scheduler.auto.start=false",
        "spring.data.redis.port=8002",
        "mysql.db.name=BootScheduledChannelTest",
        "max.workers.count=120",
        "use.system.redis=false",
        "monitor.enabled=true"
    })
@SpringBootTest
@Slf4j
@LocalTest
@SpringBootIntegrationTest
class BootScheduledChannelTest extends MessageChannelTests {

  @Test
  void publishMessageIsTriggeredOnMessageAddition() throws Exception {
    TestRunner.run(
        this::verifyPublishMessageIsTriggeredOnMessageAddition,
        () -> deleteAllMessages(emailQueue),
        () -> printQueueStats(Collections.singletonList(emailQueue)),
        3);
  }
}
