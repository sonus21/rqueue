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

package com.github.sonus21.rqueue.producer;

import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import java.util.Random;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class RqueueMessageSenderTest {
  @Rule public ExpectedException expectedException = ExpectedException.none();
  private RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
  private RqueueMessageSender rqueueMessageSender = new RqueueMessageSender(rqueueMessageTemplate);
  private String queueName = "test-queue";
  private String message = "Test Message";
  private MessageWriter messageWriter = mock(MessageWriter.class);
  private Random random = new Random();

  @Before
  public void init() throws Exception {
    writeField(rqueueMessageSender, "messageWriter", messageWriter, true);
  }

  @Test
  public void putWithNullQueueName() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("queueName can not be null");
    rqueueMessageSender.put(null, null);
  }

  @Test
  public void putWithNullMessage() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("message can not be null");
    rqueueMessageSender.put(queueName, null);
  }

  @Test
  public void put() {
    boolean returnValue = random.nextBoolean();
    doReturn(returnValue).when(messageWriter).pushMessage(queueName, message, null, null);
    assertEquals(returnValue, rqueueMessageSender.put(queueName, message));
  }

  @Test
  public void putWithRetry() {
    boolean returnValue = random.nextBoolean();
    doReturn(returnValue).when(messageWriter).pushMessage(queueName, message, 3, null);
    assertEquals(returnValue, rqueueMessageSender.put(queueName, message, 3));
  }

  @Test
  public void putWithDelay() {
    boolean returnValue = random.nextBoolean();
    doReturn(returnValue).when(messageWriter).pushMessage(queueName, message, null, 1000L);
    assertEquals(returnValue, rqueueMessageSender.put(queueName, message, 1000L));
  }

  @Test
  public void putWithDelayAndRetry() {
    boolean returnValue = random.nextBoolean();
    doReturn(returnValue).when(messageWriter).pushMessage(queueName, message, 3, 1000L);
    assertEquals(returnValue, rqueueMessageSender.put(queueName, message, 3, 1000L));
  }

  @Test
  public void moveMessageFromQueueExceptions() {
    try {
      rqueueMessageSender.moveMessageFromDeadLetterToQueue(null, queueName, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      rqueueMessageSender.moveMessageFromDeadLetterToQueue(queueName, null, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }

    try {
      rqueueMessageSender.moveMessageFromDeadLetterToQueue(queueName, queueName, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
    try {
      rqueueMessageSender.moveMessageFromDeadLetterToQueue("dlq" + queueName, queueName, -1);
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void moveMessageFromDeadLetterToQueue() {
    doReturn(true).when(rqueueMessageTemplate).moveMessage("dlq" + queueName, queueName, 100);
    rqueueMessageSender.moveMessageFromDeadLetterToQueue("dlq" + queueName, queueName, null);
  }

  @Test
  public void moveMessageFromDeadLetterToQueue2() {
    doReturn(true).when(rqueueMessageTemplate).moveMessage("dlq" + queueName, queueName, 10);
    rqueueMessageSender.moveMessageFromDeadLetterToQueue("dlq" + queueName, queueName, 10);
  }
}
