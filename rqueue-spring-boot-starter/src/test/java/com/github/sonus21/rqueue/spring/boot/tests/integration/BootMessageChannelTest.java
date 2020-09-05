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

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationListenerDisabled;
import com.github.sonus21.rqueue.test.tests.MessageChannelTest;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@ExtendWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = ApplicationListenerDisabled.class)
@TestPropertySource(
    properties = {
      "rqueue.scheduler.auto.start=false",
      "spring.redis.port=8002",
      "mysql.db.name=BootMessageChannelTest",
      "max.workers.count=120",
      "use.system.redis=false"
    })
@SpringBootTest
@Slf4j
public class BootMessageChannelTest extends MessageChannelTest {

  @Test
  public void publishMessageIsTriggeredOnMessageAddition() throws TimedOutException {
    verifyPublishMessageIsTriggeredOnMessageAddition();
  }

  @Test
  public void publishMessageIsTriggeredOnMessageRemoval() throws TimedOutException {
    verifyPublishMessageIsTriggeredOnMessageRemoval();
  }
}
