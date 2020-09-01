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
import com.github.sonus21.rqueue.spring.boot.application.Application;
import com.github.sonus21.rqueue.test.tests.MessageRetryTest;
import com.github.sonus21.test.RqueueSpringTestRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

@RunWith(RqueueSpringTestRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest
@Slf4j
@TestPropertySource(
    properties = {
      "rqueue.retry.per.poll=1000",
      "spring.redis.port=8001",
      "reservation.request.dead.letter.consumer.enabled=true",
      "reservation.request.active=true",
      "list.email.queue.enabled=true",
      "mysql.db.name=BootApplicationTest",
      "use.system.redis=false",
    })
public class ApplicationTest extends MessageRetryTest {

  @Test
  public void afterNRetryTaskIsDeletedFromProcessingQueue() throws TimedOutException {
    verifyAfterNRetryTaskIsDeletedFromProcessingQueue();
  }

  @Test
  public void messageMovedToDeadLetterQueue() throws TimedOutException {
    verifyMessageMovedToDeadLetterQueue();
  }

  @Test
  public void messageIsDiscardedAfterRetries() throws TimedOutException {
    verifyMessageIsDiscardedAfterRetries();
  }

  @Test
  public void notificationOnMessageIsTriggered() throws TimedOutException {
    verifySimpleTaskExecution();
  }

  @Test
  public void emailIsRetried() throws TimedOutException {
    verifyRetry();
  }

  @Test
  public void jobIsRetriedAndMessageIsInProcessingQueue() throws TimedOutException {
    verifyMessageIsInProcessingQueue();
  }

  @Test
  public void messageIsConsumedByDeadLetterQueueListener() throws TimedOutException {
    verifyMessageIsConsumedByDeadLetterQueueListener();
  }

  @Test
  public void testListMessageListener() throws TimedOutException {
    verifyListMessageListener();
  }
}
