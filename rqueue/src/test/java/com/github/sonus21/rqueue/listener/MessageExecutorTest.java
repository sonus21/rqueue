/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.listener;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;
import org.springframework.messaging.Message;

@Slf4j
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class MessageExecutorTest {
  RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
  RqueueMessageTemplate rqueueMessageTemplate = mock(RqueueMessageTemplate.class);
  private String queueName = "test-queue";
  private String dlqName = "test-queue-dlq";
  private String message = "This is a test message";

  @Test
  public void runMaxCheckRetryCount() {
    RqueueMessage rqueueMessage = new RqueueMessage(queueName, message, null, null);
    ConsumerQueueDetail queueDetail = new ConsumerQueueDetail(queueName, -1, "", false);
    MessageExecutor messageExecutor =
        new MessageExecutor(rqueueMessage, queueDetail, messageHandler, rqueueMessageTemplate, log);
    Integer[] counts = new Integer[1];
    counts[0] = 0;
    try {
      doAnswer(
              new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                  if (counts[0] < 10) {
                    counts[0]++;
                    throw new NullPointerException(message);
                  }
                  return null;
                }
              })
          .when(messageHandler)
          .handleMessage(any(Message.class));
    } catch (NullPointerException e) {
      // noop
    }

    messageExecutor.run();
    assertEquals(10, (int) counts[0]);
  }

  @Test
  public void runCheckMessageCountOverriding() {
    RqueueMessage rqueueMessage = new RqueueMessage(queueName, message, 3, null);
    ConsumerQueueDetail queueDetail = new ConsumerQueueDetail(queueName, 10, "", false);
    MessageExecutor messageExecutor =
        new MessageExecutor(rqueueMessage, queueDetail, messageHandler, rqueueMessageTemplate, log);
    Integer[] counts = new Integer[1];
    counts[0] = 0;
    try {
      doAnswer(
              new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                  counts[0]++;
                  throw new NullPointerException(message);
                }
              })
          .when(messageHandler)
          .handleMessage(any(Message.class));
    } catch (NullPointerException e) {
      // noop
    }
    messageExecutor.run();
    assertEquals(3, (int) counts[0]);
    verify(rqueueMessageTemplate, times(0)).add(anyString(), any(RqueueMessage.class));
  }

  @Test
  public void runCheckMessageDlq() {
    RqueueMessage rqueueMessage = new RqueueMessage(queueName, message, 3, null);
    ConsumerQueueDetail queueDetail = new ConsumerQueueDetail(queueName, 1, dlqName, false);
    MessageExecutor messageExecutor =
        new MessageExecutor(rqueueMessage, queueDetail, messageHandler, rqueueMessageTemplate, log);
    Integer[] counts = new Integer[1];
    counts[0] = 0;
    doNothing().when(rqueueMessageTemplate).add(eq(dlqName), any(RqueueMessage.class));
    try {
      doAnswer(
              new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                  counts[0]++;
                  throw new NullPointerException(message);
                }
              })
          .when(messageHandler)
          .handleMessage(any(Message.class));
    } catch (NullPointerException e) {
      // noop
    }
    messageExecutor.run();
    assertEquals(3, (int) counts[0]);
    verify(rqueueMessageTemplate, times(1)).add(anyString(), any(RqueueMessage.class));
    assertNotNull(rqueueMessage.getReEnqueuedAt());
  }
}
