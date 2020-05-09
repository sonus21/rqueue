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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.application.ApplicationListenerDisabled;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.tests.SpringTestBase;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = ApplicationListenerDisabled.class)
@TestPropertySource(
    properties = {
      "rqueue.scheduler.auto.start=false",
      "spring.redis.port=8002",
      "mysql.db.name=test2"
    })
@SpringBootTest
@Slf4j
public class MessageChannelTest extends SpringTestBase {
  /**
   * This test verified whether any pending message in the delayed queue are moved or not Whenever a
   * delayed message is pushed then it's checked whether there're any pending messages on delay
   * queue. if expired delayed messages are found on the head then a message is published on delayed
   * channel.
   */
  @Test
  public void publishMessageIsTriggeredOnMessageAddition() throws TimedOutException {
    int messageCount = 200;
    String delayedQueueName = rqueueConfig.getDelayedQueueName(emailQueue);
    enqueueIn(delayedQueueName, i -> Email.newInstance(), i -> -1000L, messageCount);
    Email email = Email.newInstance();
    log.info("adding new message {}", email);
    messageSender.put(emailQueue, email, 1000L);
    waitFor(
        () -> stringRqueueRedisTemplate.getZsetSize(delayedQueueName) <= 1,
        "one or zero messages in zset");
    assertTrue(
        "Messages are correctly moved",
        stringRqueueRedisTemplate.getListSize(rqueueConfig.getQueueName(emailQueue))
            >= messageCount);
    assertEquals(messageCount + 1, messageSender.getAllMessages(emailQueue).size());
  }
}
